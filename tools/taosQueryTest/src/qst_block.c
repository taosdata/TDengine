/*
 * Copyright (c) 2026 TAOS Data, Inc.
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 */

#include "qst_block.h"

#include <limits.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "qst_crc32c.h"

/* Distribute concurrent allocators across the shared block ring. A fixed
 * scan origin makes every process repeatedly contend on the same early
 * blocks once the run reaches steady state. The hint is process-local and
 * only chooses a starting point; ownership remains decided by the atomic
 * block-state CAS below. */
static _Atomic uint64_t gQstBlockAllocHint = 0;
#include "qst_sha256.h"

static uint64_t qstBlockAllocMixedStart(uint64_t allocation_ticket) {
  uint64_t value =
      allocation_ticket ^ ((uint64_t)(uint32_t)getpid() << 32U);

  value ^= value >> 30U;
  value *= UINT64_C(0xbf58476d1ce4e5b9);
  value ^= value >> 27U;
  value *= UINT64_C(0x94d049bb133111eb);
  return value ^ (value >> 31U);
}

static EQstCode qstGetBlock(const SQstShmMapping *mapping, uint64_t offset,
                            SQstBlockHeader **out_block) {
  const SQstRegion *region = NULL;
  uint64_t relative = 0;

  if (NULL == mapping || NULL == mapping->header || NULL == mapping->base ||
      NULL == out_block) {
    return QST_E_INVALID_ARGUMENT;
  }

  region = &mapping->header->block_region;
  if (region->element_bytes < sizeof(SQstBlockHeader) ||
      region->element_count == 0 || offset < region->region_offset ||
      offset > mapping->bytes ||
      offset >= region->region_offset + region->region_bytes) {
    return QST_E_BLOCK_INVALID;
  }

  relative = offset - region->region_offset;
  if (relative % region->element_bytes != 0 ||
      relative / region->element_bytes >= region->element_count) {
    return QST_E_BLOCK_INVALID;
  }

  *out_block = (SQstBlockHeader *)((uint8_t *)mapping->base + offset);
  return QST_OK;
}

static uint8_t *qstBlockPayload(SQstBlockHeader *block) {
  return (uint8_t *)block + sizeof(*block);
}

static uint32_t qstBlockHeaderCrc(const SQstBlockHeader *block) {
  SQstBlockHeader copy = {0};

  copy.payload_capacity = block->payload_capacity;
  copy.owner_gid = block->owner_gid;
  copy.owner_generation = block->owner_generation;
  copy.payload_length = block->payload_length;
  copy.next_block_offset = block->next_block_offset;
  copy.payload_crc32c = block->payload_crc32c;
  return qstCrc32c(0, &copy, sizeof(copy));
}

static void qstBlockReset(SQstShmMapping *mapping, SQstBlockHeader *block) {
  /* Keep RECLAIMING visible while metadata is cleared. A raw memset of the
   * atomic state would publish FREE before the old owner fields disappear,
   * allowing another allocator to claim and then have its initialization
   * overwritten by this reset. */
  if (mapping == NULL || mapping->header == NULL || block == NULL) {
    return;
  }
  memset((uint8_t *)block + offsetof(SQstBlockHeader, payload_capacity), 0,
         sizeof(*block) - offsetof(SQstBlockHeader, payload_capacity));
  atomic_store_explicit(&block->state, QST_BLOCK_FREE, memory_order_release);
  (void)atomic_fetch_add_explicit(&mapping->header->free_block_count, 1,
                                  memory_order_release);
}

static int32_t qstReclaimWritingBlock(SQstBlockHeader *block) {
  uint32_t expected = QST_BLOCK_WRITING;

  return atomic_compare_exchange_strong_explicit(
      &block->state, &expected, QST_BLOCK_RECLAIMING, memory_order_acq_rel,
      memory_order_acquire);
}

/* Releases an unpublished chain. Bounds prevent malformed links from looping. */
static void qstReleaseWritingChain(SQstShmMapping *mapping,
                                   uint64_t first_offset) {
  SQstBlockHeader *block = NULL;
  uint64_t offset = first_offset;
  uint32_t visited = 0;

  while (offset != 0 && visited < mapping->header->block_region.element_count) {
    uint64_t next = 0;
    if (qstGetBlock(mapping, offset, &block) != QST_OK) {
      return;
    }
    next = block->next_block_offset;
    if (!qstReclaimWritingBlock(block)) {
      return;
    }
    qstBlockReset(mapping, block);
    offset = next;
    ++visited;
  }
}

static void qstRestoreWritingChain(const SQstShmMapping *mapping,
                                   SQstRef ref) {
  SQstBlockHeader *block = NULL;
  uint64_t offset = ref.offset;
  uint64_t remaining = ref.length;

  while (remaining > 0 && qstGetBlock(mapping, offset, &block) == QST_OK) {
    const uint32_t consumed = remaining > block->payload_capacity ?
                                  block->payload_capacity : (uint32_t)remaining;
    uint32_t expected = QST_BLOCK_RECLAIMING;
    const uint64_t next = block->next_block_offset;

    (void)atomic_compare_exchange_strong_explicit(
        &block->state, &expected, QST_BLOCK_WRITING, memory_order_acq_rel,
        memory_order_acquire);
    remaining -= consumed;
    offset = next;
  }
}

EQstCode qst_block_alloc(SQstShmMapping *mapping, uint64_t owner_gid,
                         uint32_t owner_generation, uint64_t bytes,
                         SQstRef *out_ref) {
  const SQstRegion *region = NULL;
  uint64_t payload_capacity = 0;
  uint64_t required_blocks = 0;
  uint64_t remaining = bytes;
  uint64_t first_offset = 0;
  uint64_t previous_offset = 0;
  uint64_t start_index = 0;

  if (NULL == mapping || NULL == mapping->header || NULL == mapping->base ||
      NULL == out_ref || bytes == 0 || bytes > UINT32_MAX) {
    return QST_E_INVALID_ARGUMENT;
  }

  memset(out_ref, 0, sizeof(*out_ref));
  region = &mapping->header->block_region;
  if (region->element_bytes <= sizeof(SQstBlockHeader) ||
      region->element_count == 0) {
    return QST_E_BLOCK_NO_SPACE;
  }
  payload_capacity = region->element_bytes - sizeof(SQstBlockHeader);
  required_blocks = (bytes + payload_capacity - 1) / payload_capacity;
  if (required_blocks > region->element_count) {
    return QST_E_BLOCK_NO_SPACE;
  }

  /* Single-block records do not expose chain ordering. Spread those
   * allocations uniformly around the ring to avoid repeatedly following an
   * occupied cluster. The process identity separates forked allocators while
   * the ticket separates concurrent threads; ownership remains guarded by the
   * block-state CAS below. Retain the original origin for multi-block chains. */
  if (required_blocks == 1U) {
    const uint64_t allocation_ticket = atomic_fetch_add_explicit(
        &gQstBlockAllocHint, 1, memory_order_relaxed);

    start_index = qstBlockAllocMixedStart(allocation_ticket) %
                  region->element_count;
  }

  for (uint64_t visited = 0;
       visited < region->element_count && remaining > 0; ++visited) {
    SQstBlockHeader *block = NULL;
    const uint64_t index = required_blocks == 1U ?
                               (start_index + visited) % region->element_count :
                               visited;
    const uint64_t offset = region->region_offset + index * region->element_bytes;
    const uint32_t capacity = (uint32_t)payload_capacity;
    uint32_t expected = QST_BLOCK_FREE;

    if (qstGetBlock(mapping, offset, &block) != QST_OK) {
      qstReleaseWritingChain(mapping, first_offset);
      return QST_E_BLOCK_INVALID;
    }
    if (!atomic_compare_exchange_strong_explicit(
            &block->state, &expected, QST_BLOCK_WRITING, memory_order_acq_rel,
            memory_order_acquire)) {
      continue;
    }
    (void)atomic_fetch_sub_explicit(&mapping->header->free_block_count, 1,
                                    memory_order_acq_rel);

    block->payload_capacity = capacity;
    block->owner_gid = owner_gid;
    block->owner_generation = owner_generation;
    block->payload_length = 0;
    block->next_block_offset = 0;
    block->payload_crc32c = 0;
    block->header_crc32c = 0;

    if (previous_offset != 0) {
      SQstBlockHeader *previous = NULL;
      if (qstGetBlock(mapping, previous_offset, &previous) != QST_OK) {
        if (qstReclaimWritingBlock(block)) {
          qstBlockReset(mapping, block);
        }
        qstReleaseWritingChain(mapping, first_offset);
        return QST_E_BLOCK_INVALID;
      }
      previous->next_block_offset = offset;
    } else {
      first_offset = offset;
    }

    previous_offset = offset;
    remaining -= remaining > capacity ? capacity : remaining;
  }

  if (remaining != 0) {
    qstReleaseWritingChain(mapping, first_offset);
    return QST_E_BLOCK_NO_SPACE;
  }

  out_ref->offset = first_offset;
  out_ref->length = (uint32_t)bytes;
  return QST_OK;
}

EQstCode qst_block_write_seal(SQstShmMapping *mapping, SQstRef *inout_ref,
                              const void *source, uint64_t bytes) {
  SQstBlockHeader *block = NULL;
  uint64_t offset = 0;
  uint64_t remaining = 0;
  uint64_t copied = 0;
  uint32_t crc = 0;
  uint32_t visited = 0;

  if (NULL == mapping || NULL == inout_ref || NULL == source ||
      qstRefIsEmpty(*inout_ref) || bytes != inout_ref->length) {
    return QST_E_INVALID_ARGUMENT;
  }

  offset = inout_ref->offset;
  remaining = bytes;
  while (remaining > 0) {
    uint32_t copy_bytes = 0;

    if (qstGetBlock(mapping, offset, &block) != QST_OK ||
        ++visited > mapping->header->block_region.element_count ||
        atomic_load_explicit(&block->state, memory_order_acquire) !=
            QST_BLOCK_WRITING) {
      if (NULL != block) {
        (void)fprintf(stderr,
                      "taosQueryTest: block write-seal state mismatch "
                      "offset=%llu state=%u "
                      "owner_gid=%llu owner_generation=%u payload_capacity=%u "
                      "payload_length=%u next_offset=%llu visited=%u\n",
                      (unsigned long long)offset,
                      atomic_load_explicit(&block->state, memory_order_acquire),
                      (unsigned long long)block->owner_gid,
                      block->owner_generation, block->payload_capacity,
                      block->payload_length,
                      (unsigned long long)block->next_block_offset, visited);
      } else {
        (void)fprintf(stderr,
                      "taosQueryTest: block write-seal lookup failed "
                      "offset=%llu visited=%u\n",
                      (unsigned long long)offset, visited);
      }
      return QST_E_BLOCK_INVALID;
    }

    copy_bytes = remaining > block->payload_capacity ? block->payload_capacity :
                                                       (uint32_t)remaining;
    memcpy(qstBlockPayload(block), (const uint8_t *)source + copied,
           copy_bytes);
    block->payload_length = copy_bytes;
    block->payload_crc32c = qstCrc32c(0, qstBlockPayload(block), copy_bytes);
    block->header_crc32c = qstBlockHeaderCrc(block);
    crc = qstCrc32c(crc, qstBlockPayload(block), copy_bytes);
    copied += copy_bytes;
    remaining -= copy_bytes;
    offset = block->next_block_offset;
    if (remaining > 0 && offset == 0) {
      return QST_E_BLOCK_INVALID;
    }
  }

  if (offset != 0) {
    return QST_E_BLOCK_INVALID;
  }

  /* Find each predecessor again so the tail is published before the head. */
  for (uint32_t index = visited; index > 0; --index) {
    offset = inout_ref->offset;
    for (uint32_t position = 1; position < index; ++position) {
      if (qstGetBlock(mapping, offset, &block) != QST_OK) {
        return QST_E_BLOCK_INVALID;
      }
      offset = block->next_block_offset;
    }
    if (qstGetBlock(mapping, offset, &block) != QST_OK) {
      return QST_E_BLOCK_INVALID;
    }
    atomic_store_explicit(&block->state, QST_BLOCK_SEALED,
                          memory_order_release);
  }

  inout_ref->crc32c = crc;
  return QST_OK;
}

EQstCode qst_block_abort(SQstShmMapping *mapping, SQstRef ref,
                         uint64_t owner_gid) {
  SQstBlockHeader *block = NULL;
  uint64_t offset = 0;
  uint64_t remaining = 0;
  uint32_t visited = 0;

  if (NULL == mapping || NULL == mapping->header || qstRefIsEmpty(ref)) {
    return QST_E_INVALID_ARGUMENT;
  }
  offset = ref.offset;
  remaining = ref.length;
  while (remaining > 0) {
    uint32_t expected_length = 0;
    uint32_t state = 0;

    if (qstGetBlock(mapping, offset, &block) != QST_OK ||
        ++visited > mapping->header->block_region.element_count ||
        block->owner_gid != owner_gid ||
        block->payload_capacity != mapping->header->block_region.element_bytes -
                                       sizeof(SQstBlockHeader)) {
      return QST_E_BLOCK_INVALID;
    }
    state = atomic_load_explicit(&block->state, memory_order_acquire);
    if (state != QST_BLOCK_WRITING) {
      return QST_E_STALE_OWNER;
    }
    expected_length = remaining > block->payload_capacity ?
                          block->payload_capacity : (uint32_t)remaining;
    if (block->payload_length > expected_length) {
      return QST_E_BLOCK_INVALID;
    }
    remaining -= expected_length;
    offset = block->next_block_offset;
    if (remaining > 0 && offset == 0) {
      return QST_E_BLOCK_INVALID;
    }
  }
  if (offset != 0) {
    return QST_E_BLOCK_INVALID;
  }

  offset = ref.offset;
  remaining = ref.length;
  while (remaining > 0) {
    uint32_t expected = 0;

    if (qstGetBlock(mapping, offset, &block) != QST_OK) {
      qstRestoreWritingChain(mapping, ref);
      return QST_E_BLOCK_INVALID;
    }
    expected = atomic_load_explicit(&block->state, memory_order_acquire);
    if (expected != QST_BLOCK_WRITING ||
        !atomic_compare_exchange_strong_explicit(
            &block->state, &expected, QST_BLOCK_RECLAIMING, memory_order_acq_rel,
            memory_order_acquire)) {
      qstRestoreWritingChain(mapping, ref);
      return QST_E_STALE_OWNER;
    }
    remaining -= remaining > block->payload_capacity ?
                     block->payload_capacity : (uint32_t)remaining;
    offset = block->next_block_offset;
  }

  offset = ref.offset;
  remaining = ref.length;
  while (remaining > 0) {
    uint64_t next = 0;

    if (qstGetBlock(mapping, offset, &block) != QST_OK ||
        atomic_load_explicit(&block->state, memory_order_acquire) !=
            QST_BLOCK_RECLAIMING) {
      return QST_E_BLOCK_INVALID;
    }
    next = block->next_block_offset;
    remaining -= remaining > block->payload_capacity ?
                     block->payload_capacity : (uint32_t)remaining;
    qstBlockReset(mapping, block);
    offset = next;
  }
  return QST_OK;
}

EQstCode qst_block_validate(const SQstShmMapping *mapping, SQstRef ref,
                            uint64_t expected_owner_gid) {
  SQstBlockHeader *block = NULL;
  uint64_t offset = 0;
  uint64_t remaining = 0;
  uint32_t crc = 0;
  uint32_t visited = 0;

  if (qst_shm_validate_ref(mapping, ref) != QST_OK || qstRefIsEmpty(ref)) {
    return QST_E_BLOCK_INVALID;
  }

  offset = ref.offset;
  remaining = ref.length;
  while (remaining > 0) {
    uint32_t expected_length = 0;

    if (qstGetBlock(mapping, offset, &block) != QST_OK ||
        ++visited > mapping->header->block_region.element_count ||
        atomic_load_explicit(&block->state, memory_order_acquire) !=
            QST_BLOCK_SEALED || block->owner_gid != expected_owner_gid ||
        block->payload_capacity != mapping->header->block_region.element_bytes -
                                       sizeof(SQstBlockHeader) ||
        block->header_crc32c != qstBlockHeaderCrc(block)) {
      return QST_E_BLOCK_INVALID;
    }

    expected_length = remaining > block->payload_capacity ?
                          block->payload_capacity : (uint32_t)remaining;
    if (block->payload_length != expected_length ||
        block->payload_crc32c !=
            qstCrc32c(0, qstBlockPayload(block), block->payload_length)) {
      return QST_E_BLOCK_INVALID;
    }

    crc = qstCrc32c(crc, qstBlockPayload(block), block->payload_length);
    remaining -= block->payload_length;
    offset = block->next_block_offset;
    if (remaining > 0 && offset == 0) {
      return QST_E_BLOCK_INVALID;
    }
  }

  return offset == 0 && crc == ref.crc32c ? QST_OK : QST_E_BLOCK_INVALID;
}

EQstCode qst_block_copy(const SQstShmMapping *mapping, SQstRef ref,
                        uint64_t expected_owner_gid, uint64_t source_offset,
                        void *destination, uint64_t bytes) {
  SQstBlockHeader *block = NULL;
  uint64_t offset = 0;
  uint64_t skipped = 0;
  uint64_t copied = 0;
  uint32_t visited = 0;

  if (NULL == destination || bytes == 0 || source_offset > ref.length ||
      bytes > (uint64_t)ref.length - source_offset ||
      qst_block_validate(mapping, ref, expected_owner_gid) != QST_OK) {
    return QST_E_BLOCK_INVALID;
  }
  offset = ref.offset;
  while (copied < bytes) {
    uint64_t available = 0;
    uint64_t begin = 0;
    uint64_t copy_bytes = 0;

    if (qstGetBlock(mapping, offset, &block) != QST_OK ||
        ++visited > mapping->header->block_region.element_count) {
      return QST_E_BLOCK_INVALID;
    }
    available = block->payload_length;
    if (source_offset >= skipped + available) {
      skipped += available;
      offset = block->next_block_offset;
      continue;
    }
    begin = source_offset > skipped ? source_offset - skipped : 0;
    copy_bytes = available - begin;
    if (copy_bytes > bytes - copied) {
      copy_bytes = bytes - copied;
    }
    memcpy((uint8_t *)destination + copied, qstBlockPayload(block) + begin,
           (size_t)copy_bytes);
    copied += copy_bytes;
    skipped += available;
    offset = block->next_block_offset;
  }
  return QST_OK;
}

EQstCode qst_block_sha256(const SQstShmMapping *mapping, SQstRef ref,
                          uint64_t expected_owner_gid,
                          SQstDigest256 *out_digest) {
  SQstBlockHeader *block = NULL;
  SQstSha256 digest = {0};
  uint64_t offset = 0;
  uint64_t remaining = 0;
  uint32_t visited = 0;

  if (NULL == out_digest ||
      qst_block_validate(mapping, ref, expected_owner_gid) != QST_OK) {
    return QST_E_BLOCK_INVALID;
  }
  offset = ref.offset;
  remaining = ref.length;
  qst_sha256_init(&digest);
  while (remaining > 0) {
    uint64_t bytes = 0;

    if (qstGetBlock(mapping, offset, &block) != QST_OK ||
        ++visited > mapping->header->block_region.element_count ||
        block->payload_length == 0 || block->payload_length > remaining) {
      return QST_E_BLOCK_INVALID;
    }
    bytes = block->payload_length;
    qst_sha256_update(&digest, qstBlockPayload(block), bytes);
    remaining -= bytes;
    offset = block->next_block_offset;
  }
  qst_sha256_final(&digest, out_digest->bytes);
  return QST_OK;
}

static EQstCode qstBlockReleaseValidated(SQstShmMapping *mapping, SQstRef ref) {
  SQstBlockHeader *block = NULL;
  uint64_t offset = 0;
  uint64_t remaining = 0;

  offset = ref.offset;
  remaining = ref.length;
  while (remaining > 0) {
    uint32_t expected = QST_BLOCK_SEALED;
    const uint32_t consumed =
        remaining > mapping->header->block_region.element_bytes -
                        sizeof(SQstBlockHeader)
            ? mapping->header->block_region.element_bytes -
                  sizeof(SQstBlockHeader)
            : (uint32_t)remaining;
    uint64_t next = 0;

    if (qstGetBlock(mapping, offset, &block) != QST_OK ||
        !atomic_compare_exchange_strong_explicit(
            &block->state, &expected, QST_BLOCK_RECLAIMING,
            memory_order_acq_rel, memory_order_acquire)) {
      return QST_E_STALE_OWNER;
    }
    next = block->next_block_offset;
    qstBlockReset(mapping, block);
    remaining -= consumed;
    offset = next;
  }

  return QST_OK;
}

EQstCode qst_block_release(SQstShmMapping *mapping, SQstRef ref,
                           uint64_t owner_gid) {
  if (qst_block_validate(mapping, ref, owner_gid) != QST_OK) {
    return QST_E_BLOCK_INVALID;
  }
  return qstBlockReleaseValidated(mapping, ref);
}

EQstCode qst_block_release_many(SQstShmMapping *mapping, const SQstRef *refs,
                                uint32_t ref_count, uint64_t owner_gid) {
  if (NULL == mapping || NULL == refs || ref_count == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  /* Validate every chain before changing any block state. The task owner is
   * exclusive while persisted, so the validated metadata remains immutable
   * until this release batch completes. */
  for (uint32_t index = 0; index < ref_count; ++index) {
    if (!qstRefIsEmpty(refs[index]) &&
        qst_block_validate(mapping, refs[index], owner_gid) != QST_OK) {
      return QST_E_BLOCK_INVALID;
    }
  }
  for (uint32_t index = 0; index < ref_count; ++index) {
    if (!qstRefIsEmpty(refs[index]) &&
        qstBlockReleaseValidated(mapping, refs[index]) != QST_OK) {
      return QST_E_BLOCK_INVALID;
    }
  }
  return QST_OK;
}

EQstCode qst_block_free_count(const SQstShmMapping *mapping,
                              uint32_t *out_free_count) {
  const SQstRegion *region = NULL;
  uint32_t free_count = 0;

  if (NULL == mapping || NULL == mapping->header || NULL == mapping->base ||
      NULL == out_free_count) {
    return QST_E_INVALID_ARGUMENT;
  }
  region = &mapping->header->block_region;
  if (region->element_bytes <= sizeof(SQstBlockHeader) ||
      region->element_count == 0 || region->region_offset > mapping->bytes ||
      region->region_bytes > mapping->bytes - region->region_offset) {
    return QST_E_SHM_CORRUPT;
  }
  free_count = atomic_load_explicit(&mapping->header->free_block_count,
                                    memory_order_acquire);
  if (free_count > region->element_count) {
    return QST_E_SHM_CORRUPT;
  }
  *out_free_count = free_count;
  return QST_OK;
}

EQstCode qst_block_reclaim_run_owned(SQstShmMapping *mapping,
                                     uint32_t current_generation,
                                     uint32_t *out_reclaimed_count) {
  const SQstRegion *region = NULL;
  uint32_t reclaimed_count = 0;

  if (NULL == mapping || NULL == mapping->header || NULL == mapping->base ||
      current_generation == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  region = &mapping->header->block_region;
  if (region->element_bytes <= sizeof(SQstBlockHeader) ||
      region->element_count == 0 ||
      region->region_offset > mapping->bytes ||
      region->region_bytes > mapping->bytes - region->region_offset) {
    return QST_E_SHM_CORRUPT;
  }
  for (uint32_t index = 0; index < region->element_count; ++index) {
    SQstBlockHeader *block = NULL;
    uint32_t state = QST_BLOCK_FREE;
    uint32_t expected = 0;
    const uint64_t offset = region->region_offset +
                            (uint64_t)index * region->element_bytes;

    if (qstGetBlock(mapping, offset, &block) != QST_OK) {
      return QST_E_SHM_CORRUPT;
    }
    state = atomic_load_explicit(&block->state, memory_order_acquire);
    if (state == QST_BLOCK_FREE || block->owner_gid != 0 ||
        block->owner_generation == 0 ||
        block->owner_generation == current_generation) {
      continue;
    }
    if (state != QST_BLOCK_WRITING && state != QST_BLOCK_SEALED &&
        state != QST_BLOCK_RECLAIMING) {
      return QST_E_SHM_CORRUPT;
    }
    expected = state;
    if (!atomic_compare_exchange_strong_explicit(
            &block->state, &expected, QST_BLOCK_RECLAIMING,
            memory_order_acq_rel, memory_order_acquire)) {
      continue;
    }
    qstBlockReset(mapping, block);
    ++reclaimed_count;
  }
  if (NULL != out_reclaimed_count) {
    *out_reclaimed_count = reclaimed_count;
  }
  return QST_OK;
}
