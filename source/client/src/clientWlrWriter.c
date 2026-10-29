/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

/*
 * Workload Recorder -- bounded queue, admission control and writer thread
 * (DS 6.13, 6.14, 6.15, 6.16).
 *
 * Ordering model, and why it is shaped this way:
 *
 *   Events must appear on disk in eventSeq order, but payload copying must NOT
 *   happen under a global lock or every producer would serialize behind the
 *   largest STMT bind in flight. So the admission critical section only reserves
 *   (slot + pool space + eventSeq); the copy happens outside it, and the writer
 *   waits for each slot's `ready` flag in strict sequence order. That gives a
 *   total on-disk order with concurrent encoding.
 *
 *   The reservation order IS the recorded order. This is the linearization point
 *   for cross-thread calls (DS 6.13).
 *
 * All file and thread operations go through the OS abstraction: osFile.h makes
 * open/close/fsync compile errors, so this is enforced rather than conventional
 * (DS 4.3.1).
 */

#include "clientWlrInt.h"

#define WLR_SLOT_COUNT_MIN 1024
#define WLR_SLOT_COUNT_MAX 262144

/* Graceful-shutdown drain budget (DS 6.15.3). An upper bound on how long process
 * exit can be delayed waiting for reserved-but-unwritten events, not a target: the
 * drain also exits as soon as the writer stops making progress, so a healthy
 * shutdown never approaches this and a stalled one does not wait it out. */
#define WLR_DRAIN_TIMEOUT_MS 5000

/* Poll interval while draining, and how many consecutive polls with no advance in
 * commitSeq mean the writer has stopped progressing. 2ms keeps a normal drain
 * prompt; 250 polls (~500ms) is long enough that ordinary write latency or a
 * descheduled writer is not mistaken for a stall. */
#define WLR_DRAIN_POLL_MS 2
#define WLR_LIFECYCLE_CLOSING INT64_MIN
#ifdef WLR_TEST_HOOKS
#define WLR_DRAIN_STALL_POLLS 25
#define WLR_JOIN_TIMEOUT_MS 100
#else
#define WLR_DRAIN_STALL_POLLS 250
#define WLR_JOIN_TIMEOUT_MS 5000
#endif

static bool wlrLifecycleEnter(SWlrManager *mgr) {
  int64_t state = atomic_load_64(&mgr->lifecycleState);
  while (state >= 0) {
    if (atomic_val_compare_exchange_64(&mgr->lifecycleState, state, state + 1) == state) return true;
    state = atomic_load_64(&mgr->lifecycleState);
  }
  return false;
}

static void wlrLifecycleLeave(SWlrManager *mgr) {
  if (!mgr->lockReady) {
    (void)atomic_sub_fetch_64(&mgr->lifecycleState, 1);
    return;
  }
  (void)taosThreadMutexLock(&mgr->lock);
  (void)atomic_sub_fetch_64(&mgr->lifecycleState, 1);
  (void)taosThreadCondBroadcast(&mgr->spaceCond);
  (void)taosThreadMutexUnlock(&mgr->lock);
}

static int32_t wlrFsyncSegment(SWlrManager *mgr) {
#ifdef WLR_TEST_HOOKS
  wlrTestWaitFinalIo(mgr, WLR_TEST_FINAL_FLUSH);
#endif
  if (atomic_load_32(&mgr->injectFsyncFail) != 0) return -1;
  return taosFsyncFile(mgr->segFile);
}

/* --------------------------------------------------------------- fail-open */

static void wlrWakeWriter(SWlrManager *mgr) {
  if (!mgr->lockReady || !mgr->queueSyncReady) return;
  (void)taosThreadMutexLock(&mgr->lock);
  (void)taosThreadCondBroadcast(&mgr->workCond);
  (void)taosThreadMutexUnlock(&mgr->lock);
}

void wlrFailOpen(SWlrManager *mgr, int32_t reason) {
  atomic_store_32(&mgr->captureIncomplete, 1);
  (void)atomic_fetch_add_64(&mgr->droppedEvents, 1);
  /* Only one thread performs the transition, but every thread that observes the
   * failure must still be released, so waking happens unconditionally below. */
  bool first = wlrTrySetState(WLR_STATE_RUNNING, WLR_STATE_FAILED_OPEN);
  if (!first) {
    /* Init cleanup owns the eventual FAILED_OPEN -> OFF transition when STARTING
     * fails; this temporary shared state exists to wake initialized waiters and
     * preserve the incomplete-capture signal before teardown. */
    first = wlrTrySetState(WLR_STATE_STARTING, WLR_STATE_FAILED_OPEN);
  }
  if (!first) {
    first = wlrTrySetState(WLR_STATE_STOPPING, WLR_STATE_FAILED_OPEN);
  }

  if (first) {
    tscError("WLR: writer failed (code:%d); recording stopped, client continues. Session is incomplete.",
             reason);
    /* Do not append a marker here: this function can run while the writer is still
     * inside a partial frame write. The writer appends its terminal marker only
     * after queued I/O ends, so direct I/O here would risk interleaving two frames. */
  }

  /* Critical: anyone blocked for pool space or a begin ack must be released, or
   * a disk failure would stall business threads forever (DS 6.13). */
  wlrQueueWakeAll(mgr);
}

void wlrQueueWakeAll(SWlrManager *mgr) {
  if (!mgr->lockReady || !mgr->queueSyncReady) return;
  (void)taosThreadMutexLock(&mgr->lock);
  (void)taosThreadCondBroadcast(&mgr->spaceCond);
  (void)taosThreadCondBroadcast(&mgr->ackCond);
  (void)taosThreadCondBroadcast(&mgr->workCond);
  (void)taosThreadMutexUnlock(&mgr->lock);
}

/* ---------------------------------------------------------------- queue init */

int32_t wlrQueueInit(SWlrManager *mgr, int64_t bufferBytes) {
  int32_t slotCount;
  int32_t ready = 0; /* how many sync primitives are initialised, for _fail */

  mgr->queueSyncReady = false;

  /* SWlrSlot.payloadOffset is uint32_t, and wlrReserve derives it from
   * head % poolSize, so every representable offset must fit in 32 bits. At the
   * configured maximum (WLR_CFG_BUFFER_MB_MAX = 4096 MiB) poolSize is exactly
   * 2^32 and the largest offset is 2^32-1 -- inside the type, but with nothing to
   * spare. Reject anything larger here rather than silently truncating the offset
   * and writing a frame from the wrong pool range. */
  if (bufferBytes <= 0 || (uint64_t)bufferBytes > (uint64_t)UINT32_MAX + 1ull ||
      (uint64_t)bufferBytes > (uint64_t)SIZE_MAX) {
    tscError("WLR: payload pool of %" PRId64 " bytes exceeds the platform-addressable range",
             bufferBytes);
    return WLR_E_CONFIG;
  }

  mgr->pool = (uint8_t *)taosMemoryMalloc((size_t)bufferBytes);
  if (mgr->pool == NULL) {
    tscError("WLR: failed to allocate %" PRId64 " byte payload pool", bufferBytes);
    return WLR_E_ALLOC;
  }
  mgr->poolSize = bufferBytes;
  mgr->poolHead = 0;
  mgr->poolTail = 0;

  /* One slot per 4KiB of budget, clamped. Sized off the budget so a small buffer
   * does not end up with a huge slot ring that can never be filled. */
  slotCount = (int32_t)(bufferBytes / 4096);
  if (slotCount < WLR_SLOT_COUNT_MIN) slotCount = WLR_SLOT_COUNT_MIN;
  if (slotCount > WLR_SLOT_COUNT_MAX) slotCount = WLR_SLOT_COUNT_MAX;

  mgr->slots = (SWlrSlot *)taosMemoryCalloc((size_t)slotCount, sizeof(SWlrSlot));
  if (mgr->slots == NULL) {
    taosMemoryFreeClear(mgr->pool);
    mgr->poolSize = 0;
    return WLR_E_ALLOC;
  }
  mgr->slotCount = slotCount;
  mgr->writeSeq = 0;
  mgr->commitSeq = 0;
  mgr->producerCount = 0;
  mgr->lifecycleState = 0;

  /* Every failure past this point must release the pool and slot ring: returning
   * early without freeing them leaks both for the process lifetime, since the
   * recorder never retries init. */
  if (taosThreadMutexInit(&mgr->admitLock, NULL) != 0) goto _fail;
  ready = 1;
  if (taosThreadMutexInit(&mgr->activeLock, NULL) != 0) goto _fail;
  ready = 2;
  mgr->activeLockReady = true;
  if (taosThreadCondInit(&mgr->spaceCond, NULL) != 0) goto _fail;
  ready = 3;
  if (taosThreadCondInit(&mgr->ackCond, NULL) != 0) goto _fail;
  ready = 4;
  if (taosThreadCondInit(&mgr->workCond, NULL) != 0) goto _fail;
  ready = 5;
  mgr->queueSyncReady = true;

  return WLR_E_OK;

_fail:
  /* Destroy in reverse order of init, guarded by how far init got. wlrQueueDestroy
   * cannot do this for us: it skips its teardown block when slots == NULL, which is
   * exactly the state left here, so anything already initialised would leak for the
   * process lifetime. Over-destroying is the risk on the other side, hence the
   * step counter rather than destroying unconditionally. */
  if (ready >= 5) (void)taosThreadCondDestroy(&mgr->workCond);
  if (ready >= 4) (void)taosThreadCondDestroy(&mgr->ackCond);
  if (ready >= 3) (void)taosThreadCondDestroy(&mgr->spaceCond);
  if (ready >= 2) (void)taosThreadMutexDestroy(&mgr->activeLock);
  if (ready >= 1) (void)taosThreadMutexDestroy(&mgr->admitLock);
  mgr->activeLockReady = false;
  taosMemoryFreeClear(mgr->pool);
  taosMemoryFreeClear(mgr->slots);
  mgr->poolSize = 0;
  mgr->slotCount = 0;
  return WLR_E_ALLOC;
}

/* Frees the pool and slot ring after wlrQueueWaitProducers has drained every
 * reservation that still owns pointers into them. */
void wlrQueueDestroy(SWlrManager *mgr) {
  mgr->queueSyncReady = false;
  mgr->activeLockReady = false;
  if (mgr->slots != NULL) {
    (void)taosThreadMutexDestroy(&mgr->admitLock);
    (void)taosThreadMutexDestroy(&mgr->activeLock);
    (void)taosThreadCondDestroy(&mgr->spaceCond);
    (void)taosThreadCondDestroy(&mgr->ackCond);
    (void)taosThreadCondDestroy(&mgr->workCond);
    taosMemoryFreeClear(mgr->slots);
  }
  taosMemoryFreeClear(mgr->pool);
  mgr->poolSize = 0;
  mgr->slotCount = 0;
}

void wlrQueueWaitProducers(SWlrManager *mgr) {
  if (!mgr->lockReady || !mgr->queueSyncReady) return;
  int64_t state = atomic_load_64(&mgr->lifecycleState);
  while (state >= 0 && atomic_val_compare_exchange_64(
                           &mgr->lifecycleState, state, state | WLR_LIFECYCLE_CLOSING) != state) {
    state = atomic_load_64(&mgr->lifecycleState);
  }
  (void)taosThreadMutexLock(&mgr->lock);
  while (atomic_load_64(&mgr->lifecycleState) != WLR_LIFECYCLE_CLOSING ||
         atomic_load_64(&mgr->producerCount) != 0) {
    (void)taosThreadCondWait(&mgr->spaceCond, &mgr->lock);
  }
  (void)taosThreadMutexUnlock(&mgr->lock);
}

/* -------------------------------------------------------------- admission */

static int64_t wlrPoolUsed(SWlrManager *mgr) {
  return atomic_load_64(&mgr->poolHead) - atomic_load_64(&mgr->poolTail);
}

static bool wlrAdmissionAvailable(SWlrManager *mgr, uint32_t payloadLen) {
  if (mgr->slots == NULL || mgr->pool == NULL || mgr->slotCount <= 0 || mgr->poolSize <= 0) {
    return false;
  }

  int64_t nextSeq = atomic_load_64(&mgr->nextEventSeq);
  int64_t inFlight = nextSeq - atomic_load_64(&mgr->commitSeq);
  int64_t slotIdx = nextSeq % mgr->slotCount;
  if (inFlight >= (int64_t)mgr->slotCount ||
      atomic_load_32(&mgr->slots[slotIdx].ackPending) != 0) {
    return false;
  }

  int64_t headOffset = atomic_load_64(&mgr->poolHead) % mgr->poolSize;
  int64_t poolBytes = (int64_t)payloadLen;
  if (headOffset + poolBytes > mgr->poolSize) {
    poolBytes += mgr->poolSize - headOffset;
  }
  return mgr->poolSize - wlrPoolUsed(mgr) >= poolBytes;
}

int32_t wlrReserve(SWlrManager *mgr, uint32_t payloadLen, uint16_t frameType, uint64_t logicalEventId,
                   SWlrSlot **slotOut, uint8_t **bufOut) {
  SWlrSlot *slot;
  int64_t   head;
  int64_t   reservationStart;
  int64_t   offset;
  int64_t   waitStart = 0;

  if (payloadLen > WLR_MAX_FRAME_PAYLOAD) return WLR_E_PAYLOAD_LIMIT;
  if (!wlrLifecycleEnter(mgr)) return WLR_E_WRITE;
  if (!mgr->queueSyncReady || mgr->slots == NULL || mgr->pool == NULL ||
      mgr->slotCount <= 0 || mgr->poolSize <= 0) {
    wlrLifecycleLeave(mgr);
    return WLR_E_WRITE;
  }

  (void)taosThreadMutexLock(&mgr->admitLock);

  /* Wait for pool space and a free slot. Condition waits may reorder producers;
   * the observed total order is defined when the winner reacquires admitLock and
   * allocates its eventSeq in the reservation critical section below (DS 6.13). */
  for (;;) {
    int32_t st = atomic_load_32(&mgr->state);
    if (st != WLR_STATE_RUNNING) {
      if (waitStart != 0) {
        (void)atomic_add_fetch_64(&mgr->backpressureNs, taosGetMonoTimestampNs() - waitStart);
      }
      (void)taosThreadMutexUnlock(&mgr->admitLock);
      wlrLifecycleLeave(mgr);
      return WLR_E_WRITE;
    }

    if (wlrAdmissionAvailable(mgr, payloadLen)) break;

    if (waitStart == 0) waitStart = taosGetMonoTimestampNs();

    /* Nudge the writer, then block. Waiting on lock (not admitLock) so the
     * writer can signal without contending for admission. */
    (void)taosThreadMutexLock(&mgr->lock);
    (void)taosThreadCondBroadcast(&mgr->workCond);
    (void)atomic_add_fetch_32(&mgr->admissionWaiters, 1);
    while (atomic_load_32(&mgr->state) == WLR_STATE_RUNNING &&
           !wlrAdmissionAvailable(mgr, payloadLen)) {
      (void)taosThreadCondWait(&mgr->spaceCond, &mgr->lock);
    }
    (void)atomic_sub_fetch_32(&mgr->admissionWaiters, 1);
    (void)taosThreadMutexUnlock(&mgr->lock);
  }

  if (waitStart != 0) {
    (void)atomic_add_fetch_64(&mgr->backpressureNs, taosGetMonoTimestampNs() - waitStart);
  }

  /* Reserve: sequence number, pool range, slot. */
  int64_t seq = atomic_add_fetch_64(&mgr->nextEventSeq, 1);
  head = atomic_load_64(&mgr->poolHead);
  reservationStart = head;
  offset = head % mgr->poolSize;

  /* Avoid a payload straddling the ring end: skip to the start instead, so the
   * writer always sees one contiguous range. */
  if (offset + (int64_t)payloadLen > mgr->poolSize) {
    head += mgr->poolSize - offset;
    offset = 0;
  }
  atomic_store_64(&mgr->poolHead, head + (int64_t)payloadLen);

  slot = &mgr->slots[(seq - 1) % mgr->slotCount];
  /* Revoke the previous publication before changing any field that the writer
   * associates with ready==1. commitSeq made this slot reusable, but ready may
   * still describe the prior event until the producer clears it here. */
  atomic_store_32(&slot->ready, 0);
  tmemory_barrier(0);
  atomic_store_64(&slot->eventSeq, seq);
  slot->logicalEventId = logicalEventId;
  slot->frameType = frameType;
  slot->flags = 0;
  slot->payloadOffset = (uint32_t)offset;
  slot->payloadLen = payloadLen;
  slot->poolBytes = (uint64_t)(head + (int64_t)payloadLen - reservationStart);
  slot->ackDone = 0;
  slot->ackCode = 0;
  (void)atomic_add_fetch_64(&mgr->producerCount, 1);

  (void)taosThreadMutexUnlock(&mgr->admitLock);
  wlrLifecycleLeave(mgr);

  *slotOut = slot;
  *bufOut = mgr->pool + offset;
  return WLR_E_OK;
}

#ifdef WLR_TEST_HOOKS
/* Reserve the zero-byte test probe without ever waiting for admission or space. */
static int32_t wlrTryReserveProbe(SWlrManager *mgr) {
  SWlrSlot *slot;
  int64_t   seq;

  if (!wlrLifecycleEnter(mgr)) return WLR_E_WRITE;
  if (!mgr->queueSyncReady || mgr->slots == NULL || mgr->pool == NULL ||
      mgr->slotCount <= 0 || mgr->poolSize <= 0) {
    wlrLifecycleLeave(mgr);
    return WLR_E_WRITE;
  }
  if (taosThreadMutexTryLock(&mgr->admitLock) != 0) {
    wlrLifecycleLeave(mgr);
    return WLR_E_WRITE;
  }
  if (atomic_load_32(&mgr->state) != WLR_STATE_RUNNING ||
      !wlrAdmissionAvailable(mgr, 0)) {
    (void)taosThreadMutexUnlock(&mgr->admitLock);
    wlrLifecycleLeave(mgr);
    return WLR_E_WRITE;
  }

  seq = atomic_add_fetch_64(&mgr->nextEventSeq, 1);
  slot = &mgr->slots[(seq - 1) % mgr->slotCount];
  atomic_store_32(&slot->ready, 0);
  tmemory_barrier(0);
  atomic_store_64(&slot->eventSeq, seq);
  slot->logicalEventId = 0;
  slot->frameType = WLR_FRAME_EVENT;
  slot->flags = 0;
  slot->payloadOffset = 0;
  slot->payloadLen = 0;
  slot->poolBytes = 0;
  slot->ackDone = 0;
  slot->ackCode = 0;
  (void)atomic_add_fetch_64(&mgr->producerCount, 1);
  /* Publish before releasing admission: another producer cannot claim the next
   * sequence while the probe remains reserved but invisible to the writer. */
  (void)wlrPublish(mgr, slot, false);
  (void)taosThreadMutexUnlock(&mgr->admitLock);
  wlrLifecycleLeave(mgr);

  return WLR_E_OK;
}
#endif

int32_t wlrPublish(SWlrManager *mgr, SWlrSlot *slot, bool waitAck) {
  int32_t code = WLR_E_OK;

  if (waitAck) atomic_store_32(&slot->ackPending, 1);
  /* Release fence: everything the caller stored into the pool must be visible to
   * the writer before it can observe ready==1. atomic_store_32 alone does not
   * guarantee this on the WINDOWS branch of osAtomic.c (plain volatile store), so
   * the barrier is explicit rather than inherited from the atomic. */
  tmemory_barrier(0);
  atomic_store_32(&slot->ready, 1);

  wlrWakeWriter(mgr);

  if (!waitAck) {
    (void)taosThreadMutexLock(&mgr->lock);
    (void)atomic_sub_fetch_64(&mgr->producerCount, 1);
    (void)taosThreadCondBroadcast(&mgr->spaceCond);
    (void)taosThreadMutexUnlock(&mgr->lock);
    return WLR_E_OK;
  }

  /* BEGIN frames only: block until the frame -- body, CRC and trailing LF -- has
   * been handed to the OS. This is what guarantees a hung call still leaves a
   * record (DS 6.14). It is an acknowledgement of write(), NOT of fsync(). */
  (void)taosThreadMutexLock(&mgr->lock);
  while (atomic_load_32(&slot->ackDone) == 0) {
    if (atomic_load_32(&mgr->state) != WLR_STATE_RUNNING) {
      code = WLR_E_WRITE;
      break;
    }
    (void)taosThreadCondWait(&mgr->ackCond, &mgr->lock);
  }
  if (code == WLR_E_OK) code = slot->ackCode;
  /* Every exit after a successful reservation must release both ownership
   * markers. Otherwise shutdown can wait forever for this producer, or slot
   * admission can remain blocked by a stale ackPending flag. */
  atomic_store_32(&slot->ackPending, 0);
  (void)atomic_sub_fetch_64(&mgr->producerCount, 1);
  (void)taosThreadCondBroadcast(&mgr->spaceCond);
  (void)taosThreadMutexUnlock(&mgr->lock);

  return code;
}

/* ----------------------------------------------------------------- segments */

static int32_t wlrOpenSegment(SWlrManager *mgr) {
  char    path[PATH_MAX + TSDB_FILENAME_LEN] = {0};
  uint8_t hdr[WLR_FILE_HEADER_SIZE];
  SWlrFileHeader fh = {0};
  int32_t n;

  for (;;) {
    n = snprintf(path, sizeof(path), "%s%s" WLR_SEGMENT_NAME_FMT, mgr->recordDir, TD_DIRSEP,
                 mgr->processStartWallNs, mgr->pid, mgr->collision, mgr->segIndex);
    if (n <= 0 || (size_t)n >= sizeof(path)) {
      tscError("WLR: segment path too long for record dir %s", mgr->recordDir);
      return WLR_E_WRITE;
    }

    /* Cross-platform private creation gives POSIX mode 0600 or a Windows private
     * DACL. Exclusive creation preserves the never-overwrite segment contract. */
    mgr->segFile = taosOpenPrivateFile(path, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_EXCL);
    if (mgr->segFile != NULL) break;

    int32_t openCode = terrno;
    bool    nameCollision = openCode == TAOS_SYSTEM_ERROR(EEXIST);
#ifdef WINDOWS
    nameCollision = nameCollision || openCode == TAOS_SYSTEM_WINAPI_ERROR(ERROR_FILE_EXISTS) ||
                    openCode == TAOS_SYSTEM_WINAPI_ERROR(ERROR_ALREADY_EXISTS);
#endif
    if (!nameCollision) {
      tscError("WLR: cannot create segment %s since %s", path, tstrerror(openCode));
      return WLR_E_WRITE;
    }

    if (mgr->collision >= 4096) {
      tscError("WLR: cannot create a unique segment in %s", mgr->recordDir);
      return WLR_E_WRITE;
    }
    mgr->collision++;
  }

  /* Remember what this segment was actually named, so the manifest does not rebuild it
   * from a counter that may have advanced since. */
  if (mgr->segIndex < WLR_MAX_TRACKED_SEGMENTS) {
    mgr->segCollisions[mgr->segIndex] = mgr->collision;
  }
  if (mgr->segIndex == 0) mgr->manifestCollision = mgr->collision;

  memcpy(fh.sessionId, mgr->sessionId, WLR_SESSION_ID_LEN);
  memcpy(fh.clockDomainId, mgr->clockDomainId, WLR_CLOCK_DOMAIN_ID_LEN);
  fh.processStartWallNs = mgr->processStartWallNs;
  fh.processStartMonoNs = mgr->processStartMonoNs;
  fh.pid = mgr->pid;
  fh.segmentIndex = mgr->segIndex;
  fh.firstEventSeq = (uint64_t)atomic_load_64(&mgr->commitSeq) + 1;

  /* The segment file is already open, so both failures below must close it before
   * returning or the descriptor leaks on every rotation attempt. */
  n = wlrEncodeFileHeader(hdr, sizeof(hdr), &fh);
  if (n <= 0) {
    (void)taosCloseFile(&mgr->segFile);
    mgr->segFile = NULL;
    return WLR_E_WRITE;
  }
  if (taosWriteFile(mgr->segFile, hdr, n) != n) {
    tscError("WLR: failed to write segment header to %s", path);
    (void)taosCloseFile(&mgr->segFile);
    mgr->segFile = NULL;
    return WLR_E_WRITE;
  }

  mgr->segBytes = n;
  tscInfo("WLR: segment opened %s", path);

  /* Refresh the manifest whenever the segment set changes, so a reader that finds
   * the session mid-flight sees every segment that exists (DS 6.15.1). */
  wlrWriteManifest(mgr, false);
  return WLR_E_OK;
}

static int32_t wlrCloseSegment(SWlrManager *mgr) {
  if (mgr->segFile == NULL) return WLR_E_OK;
  int32_t code = WLR_E_OK;
  if (wlrFsyncSegment(mgr) != 0) {
    tscError("WLR: failed to flush segment before close");
    atomic_store_32(&mgr->captureIncomplete, 1);
    code = WLR_E_WRITE;
  }
#ifdef WLR_TEST_HOOKS
  wlrTestWaitFinalIo(mgr, WLR_TEST_FINAL_CLOSE);
#endif
  (void)taosCloseFile(&mgr->segFile);
  mgr->segFile = NULL;
  return code;
}

static int32_t wlrRotateIfNeeded(SWlrManager *mgr, uint32_t nextFrameSize) {
  if (mgr->segFile == NULL) return wlrOpenSegment(mgr);

  /* Rotation happens only between whole logical events, so a single event is
   * never split across files (DS 6.15.2). An event larger than the target simply
   * makes an oversized segment. */
  if (mgr->segBytes + (int64_t)nextFrameSize <= (int64_t)WLR_SEGMENT_ROTATE_SIZE) {
    return WLR_E_OK;
  }

  if (wlrCloseSegment(mgr) != WLR_E_OK) return WLR_E_WRITE;
  int32_t savedIndex = mgr->segIndex;
  mgr->segIndex++;
  int32_t code = wlrOpenSegment(mgr);
  if (code != WLR_E_OK) mgr->segIndex = savedIndex;
  return code;
}

/* ------------------------------------------------------------- writer loop */

static int32_t wlrWriteSlot(SWlrManager *mgr, SWlrSlot *slot) {
  uint8_t         frameHdr[WLR_FRAME_OFF_PAYLOAD];
  uint8_t         tail[WLR_FRAME_CRC_SIZE + 1];
  SWlrFrameHeader fh = {0};
  uint32_t        crc;
  uint32_t        frameLength;
  int64_t         total;

  if (atomic_load_32(&mgr->injectWriteFail) != 0) return WLR_E_WRITE;
#ifdef WLR_TEST_HOOKS
  if (atomic_load_32(&mgr->stallWrite) != 0) {
    atomic_store_32(&mgr->writeStalled, 1);
    while (atomic_load_32(&mgr->stallWrite) != 0) taosMsleep(WLR_DRAIN_POLL_MS);
  }
#endif

  fh.type = slot->frameType;
  fh.flags = slot->flags;
  fh.eventSequence = atomic_load_64(&slot->eventSeq);
  fh.logicalEventId = slot->logicalEventId;
  fh.payloadLength = slot->payloadLen;

  total = (int64_t)wlrFrameEncodedSize(slot->payloadLen);
  if (wlrRotateIfNeeded(mgr, (uint32_t)total) != WLR_E_OK) return WLR_E_WRITE;

  /* Frame prefix, then payload straight from the pool, then CRC + LF. Writing
   * the payload in place avoids a second copy of large binds. */
  frameLength = (uint32_t)(WLR_FRAME_CRC_PREFIX_LEN + slot->payloadLen + WLR_FRAME_CRC_SIZE + 1);
  wlrPutU32(frameHdr + WLR_FRAME_OFF_LENGTH, frameLength);
  wlrPutU16(frameHdr + WLR_FRAME_OFF_TYPE, fh.type);
  wlrPutU16(frameHdr + WLR_FRAME_OFF_FLAGS, fh.flags);
  wlrPutU64(frameHdr + WLR_FRAME_OFF_EVENT_SEQ, fh.eventSequence);
  wlrPutU64(frameHdr + WLR_FRAME_OFF_LOGICAL_ID, fh.logicalEventId);
  wlrPutU32(frameHdr + WLR_FRAME_OFF_PAYLOAD_LEN, fh.payloadLength);

  /* CRC covers frameType..payload end, matching wlrEncodeFrame exactly. */
  crc = (*crc32c)(0, (crc_stream)(frameHdr + WLR_FRAME_OFF_TYPE), WLR_FRAME_CRC_PREFIX_LEN);
  if (slot->payloadLen > 0) {
    crc = (*crc32c)(crc, (crc_stream)(mgr->pool + slot->payloadOffset), slot->payloadLen);
  }
  wlrPutU32(tail, crc);
  tail[WLR_FRAME_CRC_SIZE] = WLR_FRAME_DELIMITER;

  if (taosWriteFile(mgr->segFile, frameHdr, sizeof(frameHdr)) != (int64_t)sizeof(frameHdr)) goto write_failed;
  if (slot->payloadLen > 0) {
    if (taosWriteFile(mgr->segFile, mgr->pool + slot->payloadOffset, slot->payloadLen) !=
        (int64_t)slot->payloadLen) {
      goto write_failed;
    }
  }
  if (taosWriteFile(mgr->segFile, tail, sizeof(tail)) != (int64_t)sizeof(tail)) goto write_failed;

  mgr->segBytes += total;
  (void)atomic_add_fetch_64(&mgr->bytesWritten, total);
  (void)atomic_add_fetch_64(&mgr->eventsWritten, 1);
  return WLR_E_OK;

write_failed:
  if (atomic_load_32(&mgr->writerDeferred) != 0) return WLR_E_WRITE;
  /* segBytes advances only after a complete frame. Remove any header/payload bytes
   * left by a failed write so a later FATAL marker remains inside the readable
   * prefix instead of being stranded behind a torn frame. If truncation or offset
   * restoration fails, close the segment: appending a marker to the damaged tail
   * would be misleading because no reader could reach it. */
  if (mgr->segFile != NULL) {
    if (taosFtruncateFile(mgr->segFile, mgr->segBytes) != 0 ||
        taosLSeekFile(mgr->segFile, mgr->segBytes, SEEK_SET) != mgr->segBytes) {
      tscError("WLR: failed to restore segment at offset:%" PRId64, mgr->segBytes);
      wlrCloseSegment(mgr);
    }
  }
  return WLR_E_WRITE;
}

static void wlrFinalizeSession(SWlrManager *mgr) {
#ifdef WLR_TEST_HOOKS
  atomic_store_32(&mgr->finalizing, 1);
#endif
  bool drained = atomic_load_64(&mgr->commitSeq) >= atomic_load_64(&mgr->nextEventSeq);
  if (!drained) atomic_store_32(&mgr->captureIncomplete, 1);
  bool graceful = drained && wlrStateIs(WLR_STATE_STOPPING) &&
                  atomic_load_32(&mgr->captureIncomplete) == 0;
  if (graceful) graceful = wlrWriteSessionStop(mgr);
  if (atomic_load_32(&mgr->writerDeferred) != 0) graceful = false;
  if (!graceful && mgr->segFile != NULL) wlrWriteFatal(mgr, WLR_E_WRITE);
  if (wlrCloseSegment(mgr) != WLR_E_OK) graceful = false;
  if (atomic_load_32(&mgr->writerDeferred) != 0) graceful = false;
  wlrWriteManifest(mgr, graceful);
}

static void *wlrWriterFunc(void *param) {
  SWlrManager *mgr = (SWlrManager *)param;
  int64_t      lastFlush = taosGetMonoTimestampMs();

  setThreadName("wlr-writer");

  while (atomic_load_32(&mgr->writerQuit) == 0) {
    int64_t   next = atomic_load_64(&mgr->writeSeq) + 1;
    SWlrSlot *slot = &mgr->slots[(next - 1) % mgr->slotCount];

    if (atomic_load_32(&mgr->stallWriter) != 0) {
      taosMsleep(10);
      continue;
    }

    /* Nothing reserved yet, or reserved but still being encoded: wait. The strict
     * sequence walk is what keeps on-disk order equal to reservation order.
     *
     * This wait is unbounded, which is safe only because reserve and publish are
     * paired on every in-process path: every wlrReserve call site reaches
     * wlrPublish on all exits, so a slot handed out is always published. Only a
     * process crash between the two could leave ready==0 forever, and that ends the
     * process regardless. Any new reserve site MUST preserve this pairing -- an
     * abandoned reservation would stall the writer permanently and, once the pool
     * fills, block every producer in the admission loop. */
    if (atomic_load_32(&slot->ready) == 0 || atomic_load_64(&slot->eventSeq) != next) {
      int64_t now = taosGetMonoTimestampMs();
      if (mgr->segFile != NULL && now - lastFlush >= WLR_WRITER_FLUSH_MS) {
        if (wlrFsyncSegment(mgr) != 0) {
          if (atomic_load_32(&mgr->writerDeferred) != 0) break;
          wlrFailOpen(mgr, WLR_E_WRITE);
          break;
        }
        lastFlush = now;
      }
      (void)taosThreadMutexLock(&mgr->lock);
      if (atomic_load_32(&mgr->writerQuit) == 0 &&
          (atomic_load_32(&slot->ready) == 0 || atomic_load_64(&slot->eventSeq) != next)) {
        int64_t nowMonoMs = taosGetMonoTimestampMs();
        int64_t waitUntil = lastFlush + WLR_WRITER_FLUSH_MS;
        int64_t waitMs = TMAX(waitUntil - nowMonoMs, 1);
#ifdef __USE_WIN_THREAD
        struct timespec deadline = {
            .tv_sec = (time_t)(waitMs / 1000),
            .tv_nsec = (long)((waitMs % 1000) * 1000000),
        };
#else
        int64_t wallDeadline = taosGetTimestampMs() + waitMs;
        struct timespec deadline = {
            .tv_sec = (time_t)(wallDeadline / 1000),
            .tv_nsec = (long)((wallDeadline % 1000) * 1000000),
        };
#endif
        (void)taosThreadCondTimedWait(&mgr->workCond, &mgr->lock, &deadline);
      }
      (void)taosThreadMutexUnlock(&mgr->lock);
      if (atomic_load_32(&mgr->writerQuit) != 0) break;
      continue;
    }

    /* Acquire fence, pairing with the release in wlrPublish: ready==1 has been
     * observed, so the producer's payload stores must become visible before they
     * are read below. Explicit for the same reason the release side is. */
    tmemory_barrier(0);

    int32_t code = wlrWriteSlot(mgr, slot);
    if (atomic_load_32(&mgr->writerDeferred) != 0) break;
    if (code == WLR_E_OK) {
      int64_t now = taosGetMonoTimestampMs();
      if (mgr->segFile != NULL && now - lastFlush >= WLR_WRITER_FLUSH_MS) {
        if (wlrFsyncSegment(mgr) != 0) {
          code = WLR_E_WRITE;
        } else {
          lastFlush = now;
        }
      }
    }
    if (atomic_load_32(&mgr->writerDeferred) != 0) break;

    (void)taosThreadMutexLock(&mgr->lock);
    slot->ackCode = code;
    atomic_store_32(&slot->ackDone, 1);
    if (code == WLR_E_OK) {
      uint64_t poolBytes = slot->poolBytes;
      atomic_store_64(&mgr->writeSeq, next);
      /* Payload bytes are now on their way to the file, so the pool range can be
       * reused by a waiting producer. */
      atomic_store_64(&mgr->poolTail, atomic_load_64(&mgr->poolTail) + (int64_t)poolBytes);
      /* commitSeq is the slot-ownership handoff to producers. Do not touch any
       * producer-owned slot field after publishing it. */
      atomic_store_32(&slot->ready, 0);
      tmemory_barrier(0);
      atomic_store_64(&mgr->commitSeq, next);
    }
    (void)taosThreadCondBroadcast(&mgr->ackCond);
    (void)taosThreadCondBroadcast(&mgr->spaceCond);
    (void)taosThreadMutexUnlock(&mgr->lock);

    if (code != WLR_E_OK) {
      wlrFailOpen(mgr, code);
      break;
    }
  }

  (void)taosThreadMutexLock(&mgr->lock);
  while (atomic_load_32(&mgr->writerQuit) == 0) {
    (void)taosThreadCondWait(&mgr->workCond, &mgr->lock);
  }
  (void)taosThreadMutexUnlock(&mgr->lock);
  if (atomic_load_32(&mgr->writerFinalize) != 0 && atomic_load_32(&mgr->writerDeferred) == 0) {
    wlrFinalizeSession(mgr);
  }
  atomic_store_32(&mgr->writerExited, 1);
  return NULL;
}

int32_t wlrWriterStart(SWlrManager *mgr) {
  TdThreadAttr attr;

  mgr->segIndex = 0;
  mgr->collision = 0;
  atomic_store_32(&mgr->writerQuit, 0);
  atomic_store_32(&mgr->writerExited, 0);
  atomic_store_32(&mgr->writerDeferred, 0);
  atomic_store_32(&mgr->writerFinalize, 0);
  atomic_store_32(&mgr->writerCloseHandedOff, 0);
  atomic_store_32(&mgr->deferredLogUsers, 0);
#ifdef WLR_TEST_HOOKS
  atomic_store_32(&mgr->finalizing, 0);
  atomic_store_32(&mgr->stallFinalIo, 0);
  atomic_store_32(&mgr->finalIoStalled, 0);
#endif

  /* First segment is created on this thread so that an unwritable directory is
   * reported as an init failure rather than surfacing later as a fail-open
   * (DS 6.3: RUNNING means the header is already on disk). */
  if (wlrOpenSegment(mgr) != WLR_E_OK) return WLR_E_WRITE;

  if (taosThreadAttrInit(&attr) != 0) {
    wlrCloseSegment(mgr);
    return WLR_E_ALLOC;
  }
  (void)taosThreadAttrSetDetachState(&attr, PTHREAD_CREATE_JOINABLE);
  if (taosThreadCreate(&mgr->writer, &attr, wlrWriterFunc, mgr) != 0) {
    (void)taosThreadAttrDestroy(&attr);
    wlrCloseSegment(mgr);
    return WLR_E_ALLOC;
  }
  (void)taosThreadAttrDestroy(&attr);
  mgr->writerReady = true;
  return WLR_E_OK;
}

static void *wlrFinishDeferredStop(void *param) {
  SWlrManager *mgr = (SWlrManager *)param;
  (void)taosThreadJoin(mgr->writer, NULL);
  while (atomic_load_32(&mgr->writerCloseHandedOff) == 0) taosMsleep(WLR_DRAIN_POLL_MS);
  mgr->writerReady = false;
  if (mgr->segFile != NULL) (void)taosCloseFile(&mgr->segFile);
  wlrWriteManifest(mgr, false);
  wlrQueueDestroy(mgr);
  if (mgr->lockReady) {
    (void)taosThreadMutexDestroy(&mgr->lock);
    mgr->lockReady = false;
  }
  atomic_store_32(&mgr->closeRequested, 0);
#ifndef TAOSD_INTEGRATED
  if (atomic_sub_fetch_32(&mgr->deferredLogUsers, 1) == 0) taosCloseLog();
#endif
  atomic_store_32(&mgr->state, WLR_STATE_OFF);
  return NULL;
}

bool wlrWriterReleaseLog(SWlrManager *mgr) {
  if (atomic_load_32(&mgr->writerDeferred) == 0) return false;
  if (atomic_sub_fetch_32(&mgr->deferredLogUsers, 1) == 0) return false;
  return true;
}

bool wlrWriterStop(SWlrManager *mgr, bool graceful) {
  if (!mgr->writerReady) {
    return !graceful;
  }

  if (graceful) {
    /* Drain what producers already reserved. Bounded: a stuck encoder must not
     * hold process exit forever, so this gives up after a fixed wait and lets
     * the tail frame be recovered by prefix scan instead (DS 6.15.3).
     *
     * Progress is tracked, not just elapsed time: a writer blocked for a reason
     * other than fail-open (a hung write, a producer that reserved a slot and
     * stalled before publishing) would otherwise burn the full timeout on every
     * shutdown. Once commitSeq has not advanced for WLR_DRAIN_STALL_POLLS polls,
     * further waiting cannot help, so stop and report the same missing-tail
     * outcome the timeout reports. */
    int64_t deadline = taosGetMonoTimestampMs() + WLR_DRAIN_TIMEOUT_MS;
    int64_t lastCommit = atomic_load_64(&mgr->commitSeq);
    int32_t idlePolls = 0;

    while (atomic_load_64(&mgr->commitSeq) < atomic_load_64(&mgr->nextEventSeq)) {
      /* A writer that has failed open will never advance commitSeq, so waiting for it
       * would burn the whole timeout on every such shutdown and then blame a "drain
       * timeout" for a failure that already happened. Stop as soon as progress has
       * become impossible. */
      if (wlrStateIs(WLR_STATE_FAILED_OPEN)) {
        tscInfo("WLR: drain stopped early, session had already failed open; %" PRId64
                " event(s) not written",
                atomic_load_64(&mgr->nextEventSeq) - atomic_load_64(&mgr->commitSeq));
        break;
      }
      if (taosGetMonoTimestampMs() > deadline) {
        tscWarn("WLR: drain timed out, %" PRId64 " event(s) may be missing from the tail",
                atomic_load_64(&mgr->nextEventSeq) - atomic_load_64(&mgr->commitSeq));
        break;
      }
      if (idlePolls >= WLR_DRAIN_STALL_POLLS) {
        tscWarn("WLR: drain stopped, writer made no progress for %dms; %" PRId64
                " event(s) may be missing from the tail",
                WLR_DRAIN_STALL_POLLS * WLR_DRAIN_POLL_MS,
                atomic_load_64(&mgr->nextEventSeq) - atomic_load_64(&mgr->commitSeq));
        break;
      }

      wlrWakeWriter(mgr);
      taosMsleep(WLR_DRAIN_POLL_MS);

      int64_t nowCommit = atomic_load_64(&mgr->commitSeq);
      if (nowCommit != lastCommit) {
        lastCommit = nowCommit;
        idlePolls = 0;
      } else {
        idlePolls++;
      }
    }
  }

  atomic_store_32(&mgr->writerQuit, 1);
  wlrQueueWakeAll(mgr);
  int64_t joinDeadline = taosGetMonoTimestampMs() + WLR_JOIN_TIMEOUT_MS;
  while (atomic_load_32(&mgr->writerExited) == 0 && taosGetMonoTimestampMs() < joinDeadline) {
    taosMsleep(WLR_DRAIN_POLL_MS);
  }
  if (atomic_load_32(&mgr->writerExited) == 0) {
    TdThreadAttr attr;
    TdThread reaper;
    atomic_store_32(&mgr->captureIncomplete, 1);
    atomic_store_32(&mgr->writerDeferred, 1);
    atomic_store_32(&mgr->deferredLogUsers, 2);
    if (taosThreadAttrInit(&attr) == 0) {
      (void)taosThreadAttrSetDetachState(&attr, PTHREAD_CREATE_DETACHED);
      if (taosThreadCreate(&reaper, &attr, wlrFinishDeferredStop, mgr) == 0) {
        (void)taosThreadAttrDestroy(&attr);
        return false;
      }
      (void)taosThreadAttrDestroy(&attr);
    }
    (void)taosThreadDetach(mgr->writer);
    tscError("WLR: unable to start deferred cleanup; recorder resources retained until process exit");
    return false;
  }
  (void)taosThreadJoin(mgr->writer, NULL);
  mgr->writerReady = false;

  return !graceful || atomic_load_32(&mgr->captureIncomplete) == 0;
}

int32_t wlrCloseSegmentPublic(SWlrManager *mgr) { return wlrCloseSegment(mgr); }

/* ------------------------------------------------------------- test hooks */

#ifdef WLR_TEST_HOOKS
void wlrTestWaitFinalIo(SWlrManager *mgr, int32_t stage) {
  if (atomic_load_32(&mgr->finalizing) == 0 || atomic_load_32(&mgr->stallFinalIo) != stage) return;
  atomic_store_32(&mgr->finalIoStalled, stage);
  while (atomic_load_32(&mgr->stallFinalIo) == stage) taosMsleep(WLR_DRAIN_POLL_MS);
}

int32_t wlrTestStallWrite(void) {
  atomic_store_32(&wlrMgr.writeStalled, 0);
  atomic_store_32(&wlrMgr.stallWrite, 1);
  return 0;
}

int32_t wlrTestResumeWrite(void) {
  atomic_store_32(&wlrMgr.stallWrite, 0);
  return 0;
}

int32_t wlrTestWriteStalled(void) { return atomic_load_32(&wlrMgr.writeStalled); }

int32_t wlrTestInjectWriterFailure(void) {
  if (!wlrStateIs(WLR_STATE_RUNNING)) {
    tscWarn("WLR: writer failure injection requires RUNNING state, current:%d", wlrRecorderState());
    return -1;
  }
  atomic_store_32(&wlrMgr.injectWriteFail, 1);

  /* Setting the flag alone is not enough: the writer only consults it on the
   * write path, so an idle writer with an empty queue would never observe it.
   * Publish a zero-length probe so the failure is deterministic.
   *
   * The probe is best-effort and must NOT go through the blocking reserve path:
   * when the pool is already full (a stalled writer with waiting producers) that
   * would park the injector on the very condition it is meant to break. */
  (void)wlrTryReserveProbe(&wlrMgr);

  wlrWakeWriter(&wlrMgr);

  for (int32_t i = 0; i < 250; ++i) {
    if (wlrRecorderState() == WLR_STATE_FAILED_OPEN) return 0;
    taosMsleep(2);
  }

  /* The writer could not reach its write path (stalled, or nothing queued), so drive
   * the transition directly. Waiters must be released either way.
   *
   * Changing state here does not touch segFile because the writer thread is still
   * alive and may be mid-frame inside wlrWriteSlot. The writer appends FATAL during
   * finalization after queued I/O ends, avoiding interleaved frames. */
  bool first = wlrTrySetState(WLR_STATE_RUNNING, WLR_STATE_FAILED_OPEN);
  if (first) {
    tscError("WLR: writer failure injected; recording stopped. Session is incomplete.");
  }
  wlrQueueWakeAll(&wlrMgr);
  return 0;
}

int32_t wlrTestInjectFsyncFailure(void) {
  if (!wlrStateIs(WLR_STATE_RUNNING)) return -1;
  atomic_store_32(&wlrMgr.injectFsyncFail, 1);
  return 0;
}

int32_t wlrTestClearWriterFailure(void) {
  atomic_store_32(&wlrMgr.injectWriteFail, 0);
  atomic_store_32(&wlrMgr.injectFsyncFail, 0);
  return 0;
}

int32_t wlrTestStallWriter(void) {
  atomic_store_32(&wlrMgr.stallWriter, 1);
  return 0;
}

int32_t wlrTestSetStopping(void) {
  return wlrTrySetState(WLR_STATE_RUNNING, WLR_STATE_STOPPING) ? 0 : -1;
}

int32_t wlrTestResumeWriter(void) {
  atomic_store_32(&wlrMgr.stallWriter, 0);
  wlrWakeWriter(&wlrMgr);
  return 0;
}

int32_t wlrTestEnterLifecycle(void) { return wlrLifecycleEnter(&wlrMgr) ? 0 : -1; }

void wlrTestLeaveLifecycle(void) { wlrLifecycleLeave(&wlrMgr); }

void wlrTestCloseLifecycleAdmission(void) { wlrQueueWaitProducers(&wlrMgr); }
#endif

/* ------------------------------------------------------- active call list */

void wlrActiveAdd(SWlrManager *mgr, uint64_t callId) {
  if (!wlrLifecycleEnter(mgr)) return;
  if (!mgr->activeLockReady) {
    wlrLifecycleLeave(mgr);
    return;
  }
  (void)taosThreadMutexLock(&mgr->activeLock);
  if (mgr->activeCount < WLR_MAX_ACTIVE_CALLS) {
    mgr->activeCalls[mgr->activeCount] = callId;
    mgr->activeCount++;
  } else {
    /* Reported rather than silently dropped: a truncated unfinished-call list would
     * make a hung call look like it completed, which is the opposite of the signal
     * this list exists to carry.
     *
     * Logged on the transition, not just at SESSION_STOP: an operator debugging a
     * hung client on a very hot connection needs to know that tracking went partial
     * WHILE it is happening, not only once the session ends -- and a session that
     * never stops cleanly would otherwise never say so. Guarded by the flag's own
     * transition so a client permanently above the cap logs once, not per call. */
    if (atomic_val_compare_exchange_32(&mgr->activeOverflow, 0, 1) == 0) {
      tscWarn("WLR: more than %d concurrent in-flight calls; the unfinished-call list "
              "is now partial and some hung calls may not be listed",
              WLR_MAX_ACTIVE_CALLS);
    }
  }
  (void)taosThreadMutexUnlock(&mgr->activeLock);
  wlrLifecycleLeave(mgr);
}

void wlrActiveRemove(SWlrManager *mgr, uint64_t callId) {
  int32_t i;
  if (!wlrLifecycleEnter(mgr)) return;
  if (!mgr->activeLockReady) {
    wlrLifecycleLeave(mgr);
    return;
  }
  (void)taosThreadMutexLock(&mgr->activeLock);
  for (i = mgr->activeCount - 1; i >= 0; --i) {
    if (mgr->activeCalls[i] == callId) {
      /* Swap with the tail: order does not matter for a set, and this keeps removal
       * O(1) on a path that runs for every completed call. */
      mgr->activeCalls[i] = mgr->activeCalls[mgr->activeCount - 1];
      mgr->activeCount--;
      break;
    }
  }
  (void)taosThreadMutexUnlock(&mgr->activeLock);
  wlrLifecycleLeave(mgr);
}

/* ---------------------------------------------------------- terminal frames */

/*
 * Write a frame directly to the segment, bypassing the queue.
 *
 * The writer emits terminal frames only after producers have left and queued I/O
 * has stopped. The close caller waits for this I/O with the writer exit deadline.
 */
static int32_t wlrWriteFrameDirect(SWlrManager *mgr, uint16_t frameType, const uint8_t *payload,
                                   uint32_t payloadLen) {
  uint8_t  frameHdr[WLR_FRAME_OFF_PAYLOAD];
  uint8_t  tail[WLR_FRAME_CRC_SIZE + 1];
  uint32_t frameLength;
  uint32_t crc;
  int64_t  seq;
  int64_t  written;

  /* Caller owns the segment exclusively; this must never overlap wlrWriteSlot. */
  if (mgr->segFile == NULL) return WLR_E_WRITE;
  if (payloadLen > WLR_MAX_FRAME_PAYLOAD || (payloadLen > 0 && payload == NULL)) {
    tscError("WLR: invalid terminal frame payload length:%u, type:%u", payloadLen, frameType);
    return WLR_E_WRITE;
  }
  if (wlrRotateIfNeeded(mgr, (uint32_t)wlrFrameEncodedSize(payloadLen)) != WLR_E_OK) return WLR_E_WRITE;

  seq = atomic_add_fetch_64(&mgr->nextEventSeq, 1);
  frameLength = (uint32_t)(WLR_FRAME_CRC_PREFIX_LEN + payloadLen + WLR_FRAME_CRC_SIZE + 1);

  wlrPutU32(frameHdr + WLR_FRAME_OFF_LENGTH, frameLength);
  wlrPutU16(frameHdr + WLR_FRAME_OFF_TYPE, frameType);
  wlrPutU16(frameHdr + WLR_FRAME_OFF_FLAGS, 0);
  wlrPutU64(frameHdr + WLR_FRAME_OFF_EVENT_SEQ, (uint64_t)seq);
  wlrPutU64(frameHdr + WLR_FRAME_OFF_LOGICAL_ID, 0);
  wlrPutU32(frameHdr + WLR_FRAME_OFF_PAYLOAD_LEN, payloadLen);

  crc = (*crc32c)(0, (crc_stream)(frameHdr + WLR_FRAME_OFF_TYPE), WLR_FRAME_CRC_PREFIX_LEN);
  if (payloadLen > 0) crc = (*crc32c)(crc, (crc_stream)payload, payloadLen);
  wlrPutU32(tail, crc);
  tail[WLR_FRAME_CRC_SIZE] = WLR_FRAME_DELIMITER;

#ifdef WLR_TEST_HOOKS
  wlrTestWaitFinalIo(mgr, WLR_TEST_FINAL_FRAME);
#endif
  written = taosWriteFile(mgr->segFile, frameHdr, sizeof(frameHdr));
  if (written != (int64_t)sizeof(frameHdr)) {
    tscError("WLR: failed to write terminal frame header, type:%u", frameType);
    goto write_failed;
  }
  if (payloadLen > 0) {
    written = taosWriteFile(mgr->segFile, payload, payloadLen);
    if (written != (int64_t)payloadLen) {
      tscError("WLR: failed to write terminal frame payload, type:%u", frameType);
      goto write_failed;
    }
  }
  written = taosWriteFile(mgr->segFile, tail, sizeof(tail));
  if (written != (int64_t)sizeof(tail)) {
    tscError("WLR: failed to write terminal frame trailer, type:%u", frameType);
    goto write_failed;
  }
  mgr->segBytes += (int64_t)wlrFrameEncodedSize(payloadLen);
  (void)atomic_add_fetch_64(&mgr->eventsWritten, 1);
  (void)atomic_add_fetch_64(&mgr->bytesWritten,
                            (int64_t)(sizeof(frameHdr) + payloadLen + sizeof(tail)));
  return WLR_E_OK;

write_failed:
  /* taosWriteFile may return an error after a partial underlying write. segBytes is
   * still the committed prefix because this frame advances it only after the trailer.
   * Roll back any partial frame, but keep the sequence permanently consumed. */
  if (mgr->segFile != NULL &&
      (taosFtruncateFile(mgr->segFile, mgr->segBytes) != 0 ||
       taosLSeekFile(mgr->segFile, mgr->segBytes, SEEK_SET) != mgr->segBytes)) {
    tscError("WLR: failed to restore segment after partial terminal frame at offset:%" PRId64
             ", type:%u", mgr->segBytes, frameType);
    wlrCloseSegment(mgr);
  }
  return WLR_E_WRITE;
}

bool wlrWriteSessionStop(SWlrManager *mgr) {
  enum {
    WLR_STOP_FIXED_TLVS = 3,
    WLR_STOP_MAX_PAYLOAD =
        (WLR_MAX_ACTIVE_CALLS + WLR_STOP_FIXED_TLVS) * (WLR_TLV_HEADER_SIZE + sizeof(uint64_t))
  };
  WLR_LAYOUT_CHECK(session_stop_payload_within_frame,
                   WLR_STOP_MAX_PAYLOAD <= WLR_MAX_FRAME_PAYLOAD);
  const size_t capacity = WLR_STOP_MAX_PAYLOAD;
  uint8_t *payload = (uint8_t *)taosMemoryMalloc(capacity);
  size_t off = 0;
  int32_t n;
  int32_t i;
  int32_t count = 0;
  int32_t encodedCount = 0;
  int32_t countEncodedLen = 0;
  bool markerWritten = false;
  size_t countOffset = 0;
  int64_t wallNs = taosGetTimestampNs();

  if (payload == NULL) {
    atomic_store_32(&mgr->captureIncomplete, 1);
    tscError("WLR: unable to allocate SESSION_STOP payload; stop marker was not written");
    return false;
  }

  n = wlrEncodeTlvU64(payload + off, capacity - off, WLR_TAG_WALL_NS, true,
                      (uint64_t)wallNs);
  if (n <= 0) goto cleanup;
  off += n;
  n = wlrEncodeTlvU64(payload + off, capacity - off, WLR_TAG_MONO_NS, true,
                      (uint64_t)taosGetMonoTimestampNs());
  if (n <= 0) goto cleanup;
  off += n;
  if (off > capacity) goto cleanup;

  if (!mgr->activeLockReady) goto cleanup;
  (void)taosThreadMutexLock(&mgr->activeLock);
  count = mgr->activeCount;
  countOffset = off;
  n = wlrEncodeTlvU64(payload + off, capacity - off, WLR_TAG_ACTIVE_CALL_COUNT, false,
                      (uint64_t)count);
  if (n <= 0) {
    tscWarn("WLR: failed to encode active-call count; SESSION_STOP marker omitted");
    (void)taosThreadMutexUnlock(&mgr->activeLock);
    goto cleanup;
  }
  countEncodedLen = n;
  off += n;
  for (i = 0; i < count; ++i) {
    n = wlrEncodeTlvU64(payload + off, capacity - off, WLR_TAG_CALL_ID, false,
                        mgr->activeCalls[i]);
    if (n <= 0) break;
    off += n;
    encodedCount++;
  }
  if (encodedCount != count) {
    int32_t rewritten = wlrEncodeTlvU64(payload + countOffset, capacity - countOffset,
                                        WLR_TAG_ACTIVE_CALL_COUNT, false, (uint64_t)encodedCount);
    if (rewritten != countEncodedLen) {
      tscWarn("WLR: failed to rewrite active-call count; SESSION_STOP marker omitted");
      (void)taosThreadMutexUnlock(&mgr->activeLock);
      goto cleanup;
    }
  }
  (void)taosThreadMutexUnlock(&mgr->activeLock);

  if (off > capacity || off > UINT32_MAX ||
      wlrWriteFrameDirect(mgr, WLR_FRAME_SESSION_STOP, payload, (uint32_t)off) != WLR_E_OK) {
    tscError("WLR: SESSION_STOP marker was not written");
    atomic_store_32(&mgr->captureIncomplete, 1);
    goto cleanup;
  }
  markerWritten = true;
  if (mgr->segFile != NULL) {
    if (wlrFsyncSegment(mgr) != 0) {
      tscError("WLR: failed to flush SESSION_STOP marker");
      atomic_store_32(&mgr->captureIncomplete, 1);
      markerWritten = false;
    }
  }

  if (encodedCount > 0) {
    tscInfo("WLR: session stopped with %d unfinished call(s); this is not corruption", encodedCount);
  }
  if (atomic_load_32(&mgr->activeOverflow) != 0) {
    tscWarn("WLR: active call list overflowed; the unfinished-call list is partial");
  }

cleanup:
  if (!markerWritten) atomic_store_32(&mgr->captureIncomplete, 1);
  taosMemoryFree(payload);
  return markerWritten;
}

void wlrWriteFatal(SWlrManager *mgr, int32_t reason) {
  uint8_t payload[256];
  int32_t off = 0;
  int32_t n;

  n = wlrEncodeTlvU64(payload + off, sizeof(payload) - off, WLR_TAG_WALL_NS, true,
                      (uint64_t)taosGetTimestampNs());
  if (n <= 0) return;
  off += n;
  n = wlrEncodeTlvU64(payload + off, sizeof(payload) - off, WLR_TAG_RET_CODE, true,
                      (uint64_t)(int64_t)reason);
  if (n <= 0) return;
  off += n;

  /* Best effort by definition: this runs because writing failed, so it may fail
   * too. The client log already carries the reason (DS 6.16). */
  (void)wlrWriteFrameDirect(mgr, WLR_FRAME_FATAL, payload, (uint32_t)off);
}
