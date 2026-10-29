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

#ifndef TDENGINE_REPLAY_LANE_H_
#define TDENGINE_REPLAY_LANE_H_

#include "replayInt.h"
#include "taos.h"

#ifdef __cplusplus
extern "C" {
#endif

#define REPLAY_MAX_HANDLES 4096
#define REPLAY_MAX_LANES   256

typedef struct {
  int32_t  sessionIdx;
  uint64_t recordedId;
  TAOS    *taos;
} SReplayConnEntry;

typedef struct {
  int32_t   sessionIdx;
  uint64_t  recordedId;
  TAOS_RES *res;
  uint64_t  connId;
  uint64_t  stmtId;
  int32_t   borrowers;
  bool      retired;
  bool      owned;
} SReplayResEntry;

typedef struct {
  int32_t     sessionIdx;
  uint64_t    recordedId;
  uint64_t    connId;
  TAOS_STMT  *stmt;
  TAOS_STMT2 *stmt2;
  int32_t     stmtColCount;
  bool        stmtShapeKnown;
  int32_t     stmt2ColCount;
  int32_t     stmt2TagCount;
  bool        stmt2ShapeKnown;
} SReplayStmtEntry;

/*
 * State every lane touches. This is a large object (hundreds of KiB because of the
 * three 4096-entry handle maps) and must be heap-allocated.
 *
 * Handles are shared on purpose: a connection used by several recorded threads must
 * be used by several lanes here, or the cross-thread contention that the recording
 * captured would not be reproduced (DS 6.19).
 *
 * Statement lookups return a borrowed raw handle without retaining it. Valid recordings
 * order STMT_CLOSE after all earlier calls on that statement complete, and the global
 * RETURN/DONE dependency watermark preserves that order across lanes. Concurrent use and
 * close of the same statement is outside the client API lifetime contract; result handles
 * use an explicit borrower count because their fetch/free lifetime is different.
 *
 * Three locks protect independent state at different rates: mapLock guards handle
 * maps, statLock guards report counters and the maxLatenessNs high-water mark,
 * and depLock guards dependency progress and abort state. Keeping them separate
 * avoids making frequent lookups contend with
 * statistics and dependency updates.
 */
typedef struct {
  SReplayConnEntry conns[REPLAY_MAX_HANDLES];
  int32_t          connCount;
  SReplayResEntry  results[REPLAY_MAX_HANDLES];
  int32_t          resultCount;
  SReplayStmtEntry stmts[REPLAY_MAX_HANDLES];
  int32_t          stmtCount;
  /* Set the first time a map fills, so the truncation is reported once instead of per
   * rejected handle. Guarded by mapLock like the maps themselves.
   *
   * Connection, statement, and result entries all use swap-remove on deletion;
   * each map remains dense in [0..count), so slots can be reused. */
  bool             connMapFullReported;
  bool             stmtMapFullReported;
  /* Results too: replayLaneAddResult frees the handle on overflow, which is correct but
   * was silent -- result truncation was the one kind an operator could not detect. */
  bool             resultMapFullReported;
  /* Set while the unfinished-call watchdog invokes taos_kill_query on a snapshot.
   * Connection removal waits so no snapped handle can be closed underneath it;
   * cancelWaitReported keeps the abnormal wait diagnostic to one line. */
  bool             cancellingQueries;
  bool             cancelWaitReported;
  int32_t          connDropsInFlight;
  int32_t          resultFreesInFlight;
  /* Abort mirror guarded by mapLock. It wakes handle-lifetime waiters without
   * reading depLock-protected state under the opposite lock order. */
  bool             mapAborted;
  TdThreadMutex    mapLock;
  TdThreadCond     mapCond;

  int32_t       executed;
  int32_t       skipped;
  int32_t       errors;
  int32_t       codeMismatches;
  int32_t       countMismatches;
  int64_t       maxLatenessNs;
  bool           pacingWarned;
  TdThreadMutex statLock;

  /* Shared time base. Initialized once before any lane starts and read-only thereafter.
   * Every lane converts recorded time against these two values, keeping lanes aligned. */
  int64_t startMonoNs;
  int64_t originMonoNs;

  /* aborted, abortCode, and replayDone are guarded by depLock while lanes/watchdog run
   * and must not be accessed bare during that period. After all workers join, the
   * owner may read the final values without locking. Set aborted when any lane hits a
   * fatal condition (bad credentials, unmappable endpoint).
   * Lanes check it and drain instead of waiting, so the run ends with a report rather
   * than hanging on a dependency that will never be satisfied. */
  int32_t          aborted;
  /* Exit code of whichever lane aborted first. Lanes cannot return values, so the
   * fatal code is published here; without it a credentials failure would be reported
   * as the generic "completed with errors" (FS 4.12). */
  int32_t          abortCode;
  bool             replayDone;
  int64_t          unfinishedDeadlineMs[REPLAY_MAX_LANES];
  /* Contiguous watermark over recorded RETURN/DONE order. A BEGIN waits only for
   * returns that preceded it in the recording; calls whose intervals overlapped do
   * not become dependencies and can execute concurrently on separate lanes. */
  int32_t          completedReturnUpto;
  TdThreadMutex    depLock;
  TdThreadCond     depCond;
  /* Indexed by absolute session event index. waitForReturnRank says which recorded
   * return watermark a BEGIN requires; returnRankByEvent maps that BEGIN to its own
   * recorded RETURN/DONE rank, or -1 for an unfinished call. completedReturns is
   * written and read only under depLock. */
  int32_t         *waitForReturnRank;
  int32_t         *returnRankByEvent;
  int32_t         *terminalByBegin;
  bool            *resultUsedByEvent;
  bool            *completedReturns;
  int32_t         *closePending;
  int32_t         *dependentHead;
  int32_t         *dependentNext;
  int32_t         *dependentClose;
  int32_t          dependentCount;
  int32_t          dependentCapacity;
  int32_t          eventTotal;
  int32_t          returnCount;

  const SReplayOpts    *opts;
  const SReplaySession *session;
} SReplayShared;
#ifdef __cplusplus
static_assert(sizeof(SReplayShared) <= 1024 * 1024, "SReplayShared exceeds heap budget");
#else
_Static_assert(sizeof(SReplayShared) <= 1024 * 1024, "SReplayShared exceeds heap budget");
#endif

/* One lane: the events recorded on a single original thread, in order. */
typedef struct {
  int32_t        sessionIdx;
  uint64_t       originTid;
  int32_t       *eventIdx; /* indices into session->events, ascending */
  int32_t        eventCount;
  int32_t        eventCap;
  int32_t        watchdogSlot;
  TdThread       thread;
  bool           started;
  /* Borrowed, not owned. The launcher must keep shared (and its borrowed opts/session)
   * alive until this lane has joined; only then may dependency state or shared be freed. */
  SReplayShared *shared;
} SReplayLane;

typedef enum {
  REPLAY_CONN_DROP_MISSING = 0,
  REPLAY_CONN_DROP_REMOVED,
  REPLAY_CONN_DROP_DEFERRED,
} EReplayConnDropResult;

/* handle map access, all mutex-guarded */
TAOS      *replayLaneFindConn(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId);
/* Consumes taos: stores it and returns true on success, or closes it and returns
 * false when the map is full. The caller must not close the handle again. */
bool       replayLaneAddConn(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId, TAOS *taos);
/* May be called only by a dispatched event after replayLaneWaitDeps, or after all
 * lanes have joined. During watchdog cancellation the mapped connection is retained
 * for teardown and DEFERRED is returned; REMOVED transfers permission to close it now. */
EReplayConnDropResult replayLaneDropConn(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId);
/* For a non-NULL handle, ownership always transfers: the map owns it on true, or this
 * function frees it on false, so the caller must not free it in either case. A NULL
 * handle is rejected without changing the map or transferring ownership. connId keys
 * cascading cleanup. */
bool       replayLaneAddResult(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId,
                               TAOS_RES *res, uint64_t connId);
bool       replayLaneAddBorrowedResult(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId,
                                       TAOS_RES *res, uint64_t connId, uint64_t stmtId);
/* Acquire/release protects a result across the complete client API call. A concurrent
 * FREE_RESULT or connection close marks it retired and defers destruction until the
 * last borrower releases it. */
TAOS_RES  *replayLaneAcquireResult(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId);
void       replayLaneReleaseResult(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId,
                                   TAOS_RES *res);
/* Marks a result retired and frees it immediately when unborrowed, otherwise on the
 * final release. Returns false when the handle was unknown or already retired. */
bool       replayLaneRetireResult(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId);
TAOS_STMT  *replayLaneFindStmt(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId);
TAOS_STMT  *replayLaneFindStmtWithConn(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId,
                                       uint64_t *connId);
void        replayLaneSetStmtShape(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId,
                                   int32_t colCount, bool known);
bool        replayLaneGetStmtShape(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId,
                                   int32_t *colCount);
TAOS_STMT2 *replayLaneFindStmt2(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId);
TAOS_STMT2 *replayLaneFindStmt2WithConn(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId,
                                        uint64_t *connId);
void        replayLaneSetStmt2Shape(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId,
                                    int32_t colCount, int32_t tagCount, bool known);
bool        replayLaneGetStmt2Shape(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId,
                                    int32_t *colCount, int32_t *tagCount);
/* Returns false when the statement map is full; in that case the passed handles
 * are closed here and ownership is not taken. The caller must not close them again. */
bool        replayLaneAddStmt(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId,
                              uint64_t connId, TAOS_STMT *stmt, TAOS_STMT2 *stmt2);
/* Unregisters the statement only; the caller still owns the handle and must close it.
 * Unlike replayLaneDropConn, this function does not close the statement handle. */
void        replayLaneDropStmt(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId);
/* Removes borrowed result aliases owned by a statement without freeing the handles. */
void        replayLaneDropStmtResults(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId);

/* stats and pacing */
/* Positional arguments are (executed, skipped, errors, mismatches), in that order.
 * Keep call sites explicit: transposing errors and mismatches silently changes report
 * counters and the REPLAY_EXIT_PARTIAL decision. */
void replayLaneAddStat(SReplayShared *sh, int32_t executed, int32_t skipped, int32_t errors,
                       int32_t mismatches);
void replayLaneNoteLateness(SReplayShared *sh, int64_t lateNs);
void replayLaneWaitUntil(SReplayShared *sh, int64_t eventMonoNs);

/* Build dependency state from the loaded BEGIN/RETURN intervals before lanes start.
 * An empty session succeeds without allocating arrays; no lane exists in that case,
 * so WaitDeps and MarkDone must not be called. Returns REPLAY_EXIT_OK on success,
 * REPLAY_EXIT_INPUT for malformed dependency data, or REPLAY_EXIT_INTERNAL when
 * dependency storage cannot be initialized. replayLaneFreeDeps releases the owned
 * arrays after join. */
int32_t replayLaneInitDeps(SReplayShared *sh);
void    replayLaneFreeDeps(SReplayShared *sh);

/* Requires initialized dependency state and 0 <= selfIdx < eventTotal. Blocks until
 * every call that returned before this BEGIN in the recording has completed. Returns
 * early when replayLaneAbort has fired; in that case the barrier was not established
 * and the caller must abandon this event without accessing any shared handle. An
 * invalid dependency array or index also aborts the whole replay with INTERNAL. */
void replayLaneWaitDeps(SReplayShared *sh, int32_t selfIdx);

/* Requires initialized dependency state and 0 <= selfIdx < eventTotal. Every BEGIN
 * assigned a return rank must call this exactly once, including BEGINs skipped during
 * partitioning rather than dispatched. Publishes that selfIdx finished, releasing
 * lanes whose recorded-return dependencies are met. */
void replayLaneMarkDone(SReplayShared *sh, int32_t selfIdx);

/* Abort the whole replay: releases every waiting lane. */
void replayLaneAbort(SReplayShared *sh, int32_t exitCode);
bool replayLaneAborted(SReplayShared *sh);

#ifdef __cplusplus
}
#endif

#endif /* TDENGINE_REPLAY_LANE_H_ */
