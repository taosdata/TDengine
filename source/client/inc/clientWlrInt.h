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
 * Workload Recorder -- internals shared between clientWlr.c (manager),
 * clientWlrCapture.c (hooks) and clientWlrWriter.c (queue + writer thread).
 *
 * Not visible outside those files. clientWlr.h is the interface the rest of the
 * client uses.
 */

#ifndef TDENGINE_CLIENT_WLR_INT_H_
#define TDENGINE_CLIENT_WLR_INT_H_

#include <stdint.h>

#include "clientWlr.h"

#include "clientLog.h"
#include "tcrc32c.h"
#include "tlog.h"

/* Concurrent in-flight recorded calls this session can track by ID. */
#define WLR_MAX_ACTIVE_CALLS 4096

/* Segments whose created filename is remembered for the manifest. A session rotates
 * every WLR_SEGMENT_ROTATE_SIZE, so this covers a long run; beyond it the manifest
 * reconstructs names from the current collision counter. */
#define WLR_MAX_TRACKED_SEGMENTS 1024

/* Keep this bound aligned with wlrQueueInit's bufferBytes runtime check and
 * wlrReserve/wlrTryReserveProbe's uint32_t payloadOffset storage. */
typedef char wlr_pool_offset_capacity_check[
    ((uint64_t)WLR_CFG_BUFFER_MB_MAX * 1024ull * 1024ull <= (uint64_t)UINT32_MAX + 1ull) ? 1 : -1];
typedef char wlr_frame_payload_pool_arithmetic_check[
    (WLR_MAX_FRAME_PAYLOAD <= INT64_MAX -
                                  (int64_t)WLR_CFG_BUFFER_MB_MAX * 1024ll * 1024ll)
        ? 1
        : -1];

#ifdef __cplusplus
extern "C" {
#endif

/*
 * One queue slot per event. Payload bytes live in the shared pool, not here, so
 * a slot stays small and the ring can be sized independently of payload size.
 *
 * ready is the publication flag: a producer reserves a slot inside the admission
 * critical section, then encodes its payload OUTSIDE that section and only then
 * sets ready. The writer consumes strictly in eventSeq order and waits for ready,
 * which is what lets payload copying happen concurrently while the on-disk order
 * still matches the sequence numbers (DS 6.13).
 *
 * ready therefore carries the payload bytes with it, and needs release/acquire
 * ordering to do so: the producer's stores into the pool must be visible to the
 * writer before the writer observes ready==1, or it would checksum and write
 * bytes the producer has not finished storing. atomic_store_32/atomic_load_32
 * supply that ordering (__ATOMIC_SEQ_CST) on GCC and Clang, but NOT on the
 * WINDOWS branch of osAtomic.c, where both degrade to plain volatile access with
 * no barrier. Publication and consumption therefore pair an explicit
 * tmemory_barrier() with the flag access -- see wlrPublish and wlrWriterFunc --
 * so the handoff is correct on every supported platform rather than only on the
 * compilers whose atomics happen to be strong enough.
 *
 * An acknowledged slot cannot be reused until its waiter has consumed ackCode.
 * ackPending gates reuse in admission; ack completion and waiter consumption
 * are serialized by mgr->lock.
 */
typedef struct {
  int64_t volatile eventSeq;
  uint64_t logicalEventId;
  uint16_t frameType;
  uint16_t flags;
  /* poolSize is capped at 2^32 bytes, so these offsets remain representable. */
  /* Producer-owned until ready is published; writer-owned after its acquire fence. */
  uint32_t payloadOffset; /* into the pool */
  uint32_t payloadLen;
  uint64_t poolBytes; /* payload plus any skipped bytes at ring wrap */
  int32_t volatile ready;      /* atomic publish/consume flag, paired with explicit barriers */
  int32_t volatile ackDone;    /* atomic writer store and waiter load under mgr->lock */
  int32_t volatile ackCode;    /* reserve clears before publish; other accesses hold mgr->lock */
  int32_t volatile ackPending; /* atomic admission check and waiter release */
} SWlrSlot;

typedef char wlr_slot_payload_offset_width_check[
    (sizeof(((SWlrSlot *)0)->payloadOffset) == sizeof(uint32_t)) ? 1 : -1];
typedef char wlr_slot_pool_bytes_width_check[
    (sizeof(((SWlrSlot *)0)->poolBytes) == sizeof(uint64_t)) ? 1 : -1];

typedef struct {
  /* Statically allocated singleton storage remains resident even when mode is OFF. */
  int32_t volatile state; /* EWlrState, via atomic_load_32 / CAS only */
  int32_t volatile closeRequested;
  int32_t volatile closeStarted;

  /* config snapshot */
  char    recordDir[PATH_MAX];
  int64_t bufferBytes;

  /* session identity */
  uint8_t sessionId[WLR_SESSION_ID_LEN];
  char    clockDomainId[WLR_CLOCK_DOMAIN_ID_LEN];
  int64_t processStartWallNs;
  int64_t processStartMonoNs;
  uint64_t pid;
  uint32_t collision;         /* next segment filename collision suffix */
  uint32_t manifestCollision; /* collision suffix shared by this session's manifest */

  /* monotonic counters. int64_t because the OS layer's atomics are signed; the
   * on-disk fields are unsigned and these never go negative in practice. */
  /* All counter accesses must use the os atomic helpers; ordinary ++ is not safe. */
  int64_t volatile nextEventSeq;
  int64_t volatile nextCallId;
  int64_t volatile nextConnId;
  int64_t volatile nextStmtId;
  int64_t volatile nextResultId;

  /* payload pool: one contiguous allocation, used as a ring */
  uint8_t *pool;
  SWlrSlot *slots;
  int64_t  poolSize;
  int64_t volatile poolHead; /* next free byte */
  int64_t volatile poolTail; /* oldest byte still unwritten */

  /* slot ring */
  int32_t   slotCount;
  int64_t volatile writeSeq;   /* next seq the writer will consume */
  int64_t volatile commitSeq;  /* highest seq fully written */
  int64_t volatile producerCount; /* reservations not yet released by wlrPublish */
  /* High bit closes admission; low bits count entrants. One CAS state prevents a
   * late entrant from registering after close observed zero users. */
  int64_t volatile lifecycleState;

  /* admission: producers serialize here only to reserve, never to copy */
  TdThreadMutex  admitLock;
  TdThreadMutex  lock;
  TdThreadCond   spaceCond; /* pool/slot space became available */
  TdThreadCond   ackCond;   /* a BEGIN frame reached the file */
  TdThreadCond   workCond;  /* writer has something to do */
  bool           lockReady;
  /* The queue condvars and admitLock have a shorter lifetime than lock. Clear
   * this before destroying them so wake paths cannot touch torn-down objects. */
  bool           queueSyncReady;

  /* writer thread */
  TdThread writer;
  bool     writerReady;
  int32_t volatile writerExited;
  int32_t volatile writerDeferred;
  int32_t volatile writerFinalize;
  int32_t volatile writerCloseHandedOff;
  int32_t volatile deferredLogUsers;
  /* Volatile manager counters and flags shared with the writer are accessed only
   * through the os atomic helpers after the writer thread starts. */
  int32_t volatile writerQuit;
  TdFilePtr segFile;
  int64_t   segBytes;
  uint32_t  segIndex;
  /*
   * The collision suffix each segment was actually created with.
   *
   * The manifest lists every segment by name, and rebuilding those names from the
   * CURRENT mgr->collision is wrong: the counter is bumped permanently when an
   * exclusive create loses a race, so a collision on segment 3 would make the manifest
   * rename segments 0-2 to files that do not exist. Recording the value at creation
   * keeps each entry matching what is on disk.
   *
   * Bounded rather than grown: names beyond the window are reconstructed from the
   * manifest sets segmentNamesPartial and omits names beyond the window; replay
   * must then discover the complete segment set by scanning the record directory.
   */
  /* Segment metadata and final I/O remain owned by the writer until it exits. */
  uint32_t  segCollisions[WLR_MAX_TRACKED_SEGMENTS];

  /* stats */
  int64_t volatile eventsWritten;
  int64_t volatile bytesWritten;
  int64_t volatile droppedEvents;   /* fail-open transitions observed */
  int64_t volatile backpressureNs;
  int32_t volatile admissionWaiters;
  int32_t volatile captureIncomplete; /* fail-open or shutdown left reserved events unwritten */

  /*
   * Calls that began but have not completed. SESSION_STOP carries this list, which
   * is the primary diagnostic for a hung API: a BEGIN with no END is exactly what a
   * deadlocked query leaves behind, and replay needs to know which calls those were
   * (DS 6.4, FS 4.7).
   *
   * A fixed ring, not a growing structure: this is touched on every recorded call,
   * so allocation there would put malloc in the hot path. Overflow is possible only
   * with more than WLR_MAX_ACTIVE_CALLS concurrent in-flight calls, and is reported
   * rather than silently truncating the list.
   *
   * That cap is therefore a real ceiling on this diagnostic, not just on the array:
   * a client holding more than WLR_MAX_ACTIVE_CALLS calls open at once gets a
   * SESSION_STOP whose count is saturated at the cap and whose ID list is partial, so a
   * hung call may be missing from it. The overflow is surfaced three ways -- a
   * tscWarn when it first happens (wlrActiveAdd), another at session stop, and
   * "unfinishedCallListPartial" in the manifest -- so a reader is never left to
   * assume a partial list was complete. Raising the cap costs 8 bytes per slot of
   * permanently resident memory per recording process.
   */
  uint64_t activeCalls[WLR_MAX_ACTIVE_CALLS];
  int32_t activeCount; /* protected by activeLock; do not access without it */
  int32_t volatile activeOverflow; /* sticky once any active-call list overflow occurs */
  TdThreadMutex    activeLock;
  /* Separate from lockReady: activeLock belongs to the queue and is destroyed by
   * wlrQueueDestroy, so guarding it with the manager's flag would allow use after
   * destruction during shutdown. */
  bool             activeLockReady;

  /* test fault injection */
  int32_t volatile injectWriteFail;
  int32_t volatile injectFsyncFail;
  int32_t volatile stallWriter;
#ifdef WLR_TEST_HOOKS
  int32_t volatile stallWrite;
  int32_t volatile writeStalled;
  int32_t volatile finalizing;
  int32_t volatile stallFinalIo;
  int32_t volatile finalIoStalled;
#endif
} SWlrManager;

extern SWlrManager wlrMgr;

extern threadlocal int32_t  wlrTlsDepth;
extern threadlocal uint64_t wlrTlsCallbackParent;

/* manager (clientWlr.c) */
bool    wlrStateIs(int32_t expected);
bool    wlrTrySetState(int32_t from, int32_t to);

/* queue + writer (clientWlrWriter.c) */
int32_t wlrQueueInit(SWlrManager *mgr, int64_t bufferBytes);
void    wlrQueueDestroy(SWlrManager *mgr);
void    wlrQueueWakeAll(SWlrManager *mgr);
void    wlrQueueWaitProducers(SWlrManager *mgr);
int32_t wlrWriterStart(SWlrManager *mgr);
/* Joins after queued and final I/O, or transfers ownership to deferred cleanup.
 * Callers must check writerDeferred before releasing resources. A true return
 * indicates a non-graceful stop or a graceful stop without capture loss. */
bool    wlrWriterStop(SWlrManager *mgr, bool graceful);
bool    wlrWriterReleaseLog(SWlrManager *mgr);

/*
 * Reserve a slot and pool space, assign eventSeq. Short critical section: it
 * must not copy payload bytes, or every producer would serialize behind large
 * STMT binds (DS 6.13).
 */
int32_t wlrReserve(SWlrManager *mgr, uint32_t payloadLen, uint16_t frameType, uint64_t logicalEventId,
                   SWlrSlot **slotOut, uint8_t **bufOut);

/* Publish a reserved slot and, when needAck was set, wait for it to be written. */
int32_t wlrPublish(SWlrManager *mgr, SWlrSlot *slot, bool waitAck);

/* Mark the session failed and release every waiter (DS 6.16). */
void wlrFailOpen(SWlrManager *mgr, int32_t reason);

/* Active-call bookkeeping for the SESSION_STOP list. */
void wlrActiveAdd(SWlrManager *mgr, uint64_t callId);
void wlrActiveRemove(SWlrManager *mgr, uint64_t callId);

int32_t wlrCloseSegmentPublic(SWlrManager *mgr);

#ifdef WLR_TEST_HOOKS
enum {
  WLR_TEST_FINAL_FRAME = 1,
  WLR_TEST_FINAL_FLUSH = 2,
  WLR_TEST_FINAL_CLOSE = 3,
  WLR_TEST_FINAL_MANIFEST = 4
};
void wlrTestWaitFinalIo(SWlrManager *mgr, int32_t stage);
int32_t wlrTestStallWrite(void);
int32_t wlrTestResumeWrite(void);
int32_t wlrTestWriteStalled(void);
int32_t wlrTestEncodeSingleMultiBind(void *bind, uint8_t *buf, size_t bufLen);
int32_t wlrTestEncodeStmt2ColumnBindv(void *bindv, uint8_t *buf, size_t bufLen);
uint32_t wlrTestSingleMultiBindSize(void *bind);
uint32_t wlrTestStmt2ColumnBindvSize(void *bindv);
uint32_t wlrTestEncodeStmt2RowBindv(void *bindv, int32_t colsPerTable, uint8_t *buf, uint32_t bufLen);
uint32_t wlrTestEncodeStmt2Tags(void *bindv, int32_t tagsPerTable, uint8_t *buf, uint32_t bufLen);
#endif

/* Session manifest (DS 6.15.1). Rewritten on segment creation and at stop.
 * Call only from the writer thread, or from the reaper after joining it;
 * concurrent calls would race with writer-owned segment metadata. */
void wlrWriteManifest(SWlrManager *mgr, bool gracefullyClosed);

/* Terminal frames (DS 6.4, 6.16). */
bool wlrWriteSessionStop(SWlrManager *mgr);
void wlrWriteFatal(SWlrManager *mgr, int32_t reason);

#ifdef __cplusplus
}
#endif

#endif /* TDENGINE_CLIENT_WLR_INT_H_ */
