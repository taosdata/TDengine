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
 * Worker Lanes (DS 6.19).
 *
 * This is what makes replay reproduce the original concurrency instead of merely
 * re-running the same statements. One lane per recorded thread, each issuing its own
 * calls at their recorded times, so N threads that hammered one connection
 * concurrently do so again.
 *
 * Why lanes rather than a thread pool: the unit of ordering is the recorded thread.
 * Within one thread the calls are strictly sequential and causally dependent -- a
 * fetch follows its query, an execute follows its binds. Across threads there is no
 * such constraint, and imposing one is exactly the serialisation that hides
 * concurrency bugs. A pool would let two calls from the same recorded thread run out
 * of order, which is worse than being serial.
 *
 * Three invariants, each with a specific failure it prevents:
 *
 *   1. A lane executes its own events in recorded order. Violating this breaks
 *      causality within a thread (fetch before query).
 *   2. Handle maps are shared and mutex-guarded. A connection recorded as shared
 *      across threads must be shared here, or the contention under test disappears.
 *   3. Each event waits for its own target time. Firing everything at once would
 *      change the arrival pattern, and the pattern is often the bug.
 */

#include "replayLane.h"

/* ------------------------------------------------------------------ handles */

/*
 * Every handle lookup and mutation goes through these, because lanes touch the maps
 * concurrently. The mutex is coarse -- one lock for all three maps -- which is right
 * here: lookups are short, contention is low relative to the network round trip each
 * call makes, and a finer scheme would add deadlock potential for no measurable win.
 */

TAOS *replayLaneFindConn(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId) {
  TAOS   *out = NULL;
  int32_t i;
  (void)taosThreadMutexLock(&sh->mapLock);
  for (i = 0; i < sh->connCount; ++i) {
    if (sh->conns[i].sessionIdx == sessionIdx && sh->conns[i].recordedId == recordedId &&
        sh->conns[i].taos != NULL) {
      out = sh->conns[i].taos;
      break;
    }
  }
  (void)taosThreadMutexUnlock(&sh->mapLock);
  return out;
}

bool replayLaneAddConn(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId, TAOS *taos) {
  bool full = false;
  bool report = false;
  (void)taosThreadMutexLock(&sh->mapLock);
  if (sh->connCount < REPLAY_MAX_HANDLES) {
    sh->conns[sh->connCount].sessionIdx = sessionIdx;
    sh->conns[sh->connCount].recordedId = recordedId;
    sh->conns[sh->connCount].taos = taos;
    sh->connCount++;
  } else {
    full = true;
    /* Report once. A silently unregistered connection turns every later call on it
     * into a skip, which reads as "the recording did less" rather than "the tool ran
     * out of room" -- the operator has no way to tell those apart otherwise. */
    if (!sh->connMapFullReported) {
      sh->connMapFullReported = true;
      report = true;
    }
  }
  (void)taosThreadMutexUnlock(&sh->mapLock);
  if (report) {
    (void)fprintf(stderr,
                  "replay: more than %d connections registered; later connections are not "
                  "tracked and their calls will be skipped (session=%d recordedId=%" PRIu64 ")\n",
                  REPLAY_MAX_HANDLES, sessionIdx, recordedId);
  }

  /* Closed outside the lock: nothing can reference an unregistered handle, and leaking
   * it would hold a server-side connection for the rest of the run. Mirrors what
   * replayLaneAddResult already does for results. */
  if (full && taos != NULL) taos_close(taos);
  return !full;
}

EReplayConnDropResult replayLaneDropConn(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId) {
  int32_t i;
  bool    found = false;
  /* Ordinary query results retain their own connection reference after CLOSE.
   * Only statement-owned result aliases become invalid when statements close. */
  (void)taosThreadMutexLock(&sh->mapLock);
  if (sh->cancellingQueries && !sh->cancelWaitReported) {
    sh->cancelWaitReported = true;
    (void)fprintf(stderr,
                  "replay: deferring connection close while watchdog cancellation is active\n");
  }
  if (sh->cancellingQueries) {
    /* Keep the mapping intact: final replay teardown owns and closes deferred
     * connections after watchdog cancellation has finished. */
    (void)taosThreadMutexUnlock(&sh->mapLock);
    return REPLAY_CONN_DROP_DEFERRED;
  }
  sh->connDropsInFlight++;
  for (i = 0; i < sh->connCount; ++i) {
    if (sh->conns[i].sessionIdx == sessionIdx && sh->conns[i].recordedId == recordedId) {
      found = true;
      break;
    }
  }
  if (!found) {
    sh->connDropsInFlight--;
    (void)taosThreadCondBroadcast(&sh->mapCond);
    (void)taosThreadMutexUnlock(&sh->mapLock);
    return REPLAY_CONN_DROP_MISSING;
  }
  /* Prevent new borrows of statement-owned results before closing statements. */
  for (i = 0; i < sh->resultCount; ++i) {
    if (sh->results[i].sessionIdx == sessionIdx && sh->results[i].connId == recordedId &&
        !sh->results[i].owned) {
      sh->results[i].retired = true;
    }
  }
  for (;;) {
    /* This counter is intentionally global: a driver-side result free may still
     * reach connection-owned state, so no connection is dropped while any free
     * is in flight. The conservative serialization keeps handle teardown safe. */
    bool borrowed = sh->resultFreesInFlight > 0;
    for (i = 0; i < sh->resultCount; ++i) {
      if (sh->results[i].sessionIdx == sessionIdx && sh->results[i].connId == recordedId &&
          !sh->results[i].owned && sh->results[i].borrowers > 0) {
        borrowed = true;
        break;
      }
    }
    if (!borrowed || sh->mapAborted) break;
    (void)taosThreadCondWait(&sh->mapCond, &sh->mapLock);
  }
  if (sh->mapAborted) {
    sh->connDropsInFlight--;
    (void)taosThreadCondBroadcast(&sh->mapCond);
    (void)taosThreadMutexUnlock(&sh->mapLock);
    return REPLAY_CONN_DROP_DEFERRED;
  }
  /* STMT2 owns these result handles. Remove their borrowed aliases before closing
   * the statements so neither connection cleanup nor final teardown can free them. */
  for (i = 0; i < sh->resultCount;) {
    if (sh->results[i].sessionIdx == sessionIdx && sh->results[i].connId == recordedId &&
        !sh->results[i].owned) {
      sh->results[i] = sh->results[sh->resultCount - 1];
      sh->resultCount--;
      continue;
    }
    ++i;
  }
  /* Statements are connection-owned too. Retire and close them before the
   * connection is closed, preventing later lookups from returning dangling handles. */
  for (;;) {
    TAOS_STMT *stmt = NULL;
    TAOS_STMT2 *stmt2 = NULL;
    bool stmtFound = false;
    for (i = 0; i < sh->stmtCount; ++i) {
      if (sh->stmts[i].sessionIdx != sessionIdx || sh->stmts[i].connId != recordedId) continue;
      stmt = sh->stmts[i].stmt;
      stmt2 = sh->stmts[i].stmt2;
      sh->stmts[i] = sh->stmts[sh->stmtCount - 1];
      sh->stmtCount--;
      stmtFound = true;
      break;
    }
    if (!stmtFound) break;
    (void)taosThreadMutexUnlock(&sh->mapLock);
    if (stmt != NULL) (void)taos_stmt_close(stmt);
    if (stmt2 != NULL) (void)taos_stmt2_close(stmt2);
    (void)taosThreadMutexLock(&sh->mapLock);
  }
  for (i = 0; i < sh->connCount; ++i) {
    if (sh->conns[i].sessionIdx == sessionIdx && sh->conns[i].recordedId == recordedId) {
      sh->conns[i] = sh->conns[sh->connCount - 1];
      sh->connCount--;
      break;
    }
  }
  sh->connDropsInFlight--;
  (void)taosThreadCondBroadcast(&sh->mapCond);
  (void)taosThreadMutexUnlock(&sh->mapLock);
  return REPLAY_CONN_DROP_REMOVED;
}

static bool replayLaneAddResultImpl(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId,
                                    TAOS_RES *res, uint64_t connId, uint64_t stmtId, bool owned) {
  /* NULL has no handle to release; false means ownership was not transferred. */
  if (res == NULL) return false;

  bool report = false;
  (void)taosThreadMutexLock(&sh->mapLock);
  for (int32_t i = 0; i < sh->resultCount; ++i) {
    if (sh->results[i].sessionIdx == sessionIdx && sh->results[i].recordedId == recordedId &&
        !sh->results[i].retired) {
      bool same = sh->results[i].res == res;
      (void)taosThreadMutexUnlock(&sh->mapLock);
      if (!same && owned) taos_free_result(res);
      return same;
    }
  }
  if (sh->resultCount < REPLAY_MAX_HANDLES) {
    sh->results[sh->resultCount].sessionIdx = sessionIdx;
    sh->results[sh->resultCount].recordedId = recordedId;
    sh->results[sh->resultCount].res = res;
    sh->results[sh->resultCount].connId = connId;
    sh->results[sh->resultCount].stmtId = stmtId;
    sh->results[sh->resultCount].borrowers = 0;
    sh->results[sh->resultCount].retired = false;
    sh->results[sh->resultCount].owned = owned;
    sh->resultCount++;
    (void)taosThreadMutexUnlock(&sh->mapLock);
    return true;
  }
  /* Report once, like the conn and stmt maps: freeing the handle keeps the run correct
   * but leaves later fetches on it as bare skips, which reads as "the recording did
   * less" rather than "the tool ran out of room". */
  if (!sh->resultMapFullReported) {
    sh->resultMapFullReported = true;
    report = true;
  }
  (void)taosThreadMutexUnlock(&sh->mapLock);
  if (report) {
    (void)fprintf(stderr,
                  "replay: more than %d results registered; later results are not tracked and "
                  "their fetches will be skipped (session=%d recordedId=%" PRIu64 ")\n",
                  REPLAY_MAX_HANDLES, sessionIdx, recordedId);
  }
  /* Out of slots: free outside the lock rather than leak, since nothing can
   * reference it. */
  if (owned) taos_free_result(res);
  return false;
}

bool replayLaneAddResult(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId, TAOS_RES *res,
                         uint64_t connId) {
  return replayLaneAddResultImpl(sh, sessionIdx, recordedId, res, connId, 0, true);
}

bool replayLaneAddBorrowedResult(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId,
                                 TAOS_RES *res, uint64_t connId, uint64_t stmtId) {
  return replayLaneAddResultImpl(sh, sessionIdx, recordedId, res, connId, stmtId, false);
}

TAOS_RES *replayLaneAcquireResult(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId) {
  TAOS_RES *out = NULL;
  int32_t   i;
  (void)taosThreadMutexLock(&sh->mapLock);
  if (sh->mapAborted) {
    (void)taosThreadMutexUnlock(&sh->mapLock);
    return NULL;
  }
  for (i = 0; i < sh->resultCount; ++i) {
    if (sh->results[i].sessionIdx == sessionIdx && sh->results[i].recordedId == recordedId &&
        !sh->results[i].retired) {
      out = sh->results[i].res;
      sh->results[i].borrowers++;
      break;
    }
  }
  (void)taosThreadMutexUnlock(&sh->mapLock);
  return out;
}

void replayLaneReleaseResult(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId,
                             TAOS_RES *res) {
  TAOS_RES *freeRes = NULL;
  int32_t i;
  (void)taosThreadMutexLock(&sh->mapLock);
  for (i = 0; i < sh->resultCount; ++i) {
    if (sh->results[i].sessionIdx == sessionIdx && sh->results[i].recordedId == recordedId &&
        sh->results[i].res == res) {
      if (sh->results[i].borrowers > 0) sh->results[i].borrowers--;
      if (sh->results[i].borrowers == 0) {
        (void)taosThreadCondBroadcast(&sh->mapCond);
        if (sh->results[i].retired) {
          freeRes = sh->results[i].owned ? sh->results[i].res : NULL;
          sh->results[i] = sh->results[sh->resultCount - 1];
          sh->resultCount--;
          if (freeRes != NULL) sh->resultFreesInFlight++;
        }
      }
      break;
    }
  }
  (void)taosThreadMutexUnlock(&sh->mapLock);
  if (freeRes != NULL) {
    taos_free_result(freeRes);
    (void)taosThreadMutexLock(&sh->mapLock);
    sh->resultFreesInFlight--;
    (void)taosThreadCondBroadcast(&sh->mapCond);
    (void)taosThreadMutexUnlock(&sh->mapLock);
  }
}

bool replayLaneRetireResult(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId) {
  TAOS_RES *freeRes = NULL;
  bool found = false;
  int32_t i;
  (void)taosThreadMutexLock(&sh->mapLock);
  for (i = 0; i < sh->resultCount; ++i) {
    if (sh->results[i].sessionIdx == sessionIdx && sh->results[i].recordedId == recordedId &&
        !sh->results[i].retired) {
      found = true;
      sh->results[i].retired = true;
      sh->results[i].owned = true;
      if (sh->results[i].borrowers == 0) {
        freeRes = sh->results[i].res;
        sh->results[i] = sh->results[sh->resultCount - 1];
        sh->resultCount--;
        sh->resultFreesInFlight++;
      }
      break;
    }
  }
  (void)taosThreadMutexUnlock(&sh->mapLock);
  if (freeRes != NULL) {
    taos_free_result(freeRes);
    (void)taosThreadMutexLock(&sh->mapLock);
    sh->resultFreesInFlight--;
    (void)taosThreadCondBroadcast(&sh->mapCond);
    (void)taosThreadMutexUnlock(&sh->mapLock);
  }
  return found;
}

TAOS_STMT *replayLaneFindStmt(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId) {
  return replayLaneFindStmtWithConn(sh, sessionIdx, recordedId, NULL);
}

TAOS_STMT *replayLaneFindStmtWithConn(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId,
                                      uint64_t *connId) {
  TAOS_STMT *out = NULL;
  int32_t    i;
  (void)taosThreadMutexLock(&sh->mapLock);
  for (i = 0; i < sh->stmtCount; ++i) {
    if (sh->stmts[i].sessionIdx == sessionIdx && sh->stmts[i].recordedId == recordedId) {
      out = sh->stmts[i].stmt;
      if (connId != NULL) *connId = sh->stmts[i].connId;
      break;
    }
  }
  (void)taosThreadMutexUnlock(&sh->mapLock);
  return out;
}

void replayLaneSetStmtShape(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId,
                            int32_t colCount, bool known) {
  (void)taosThreadMutexLock(&sh->mapLock);
  for (int32_t index = 0; index < sh->stmtCount; ++index) {
    SReplayStmtEntry *entry = &sh->stmts[index];
    if (entry->sessionIdx != sessionIdx || entry->recordedId != recordedId || entry->stmt == NULL) continue;
    entry->stmtColCount = colCount;
    entry->stmtShapeKnown = known;
    break;
  }
  (void)taosThreadMutexUnlock(&sh->mapLock);
}

bool replayLaneGetStmtShape(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId,
                            int32_t *colCount) {
  bool found = false;
  (void)taosThreadMutexLock(&sh->mapLock);
  for (int32_t index = 0; index < sh->stmtCount; ++index) {
    SReplayStmtEntry *entry = &sh->stmts[index];
    if (entry->sessionIdx != sessionIdx || entry->recordedId != recordedId || entry->stmt == NULL) continue;
    if (entry->stmtShapeKnown) {
      *colCount = entry->stmtColCount;
      found = true;
    }
    break;
  }
  (void)taosThreadMutexUnlock(&sh->mapLock);
  return found;
}

TAOS_STMT2 *replayLaneFindStmt2(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId) {
  return replayLaneFindStmt2WithConn(sh, sessionIdx, recordedId, NULL);
}

TAOS_STMT2 *replayLaneFindStmt2WithConn(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId,
                                        uint64_t *connId) {
  TAOS_STMT2 *out = NULL;
  int32_t     i;
  (void)taosThreadMutexLock(&sh->mapLock);
  for (i = 0; i < sh->stmtCount; ++i) {
    if (sh->stmts[i].sessionIdx == sessionIdx && sh->stmts[i].recordedId == recordedId) {
      out = sh->stmts[i].stmt2;
      if (connId != NULL) *connId = sh->stmts[i].connId;
      break;
    }
  }
  (void)taosThreadMutexUnlock(&sh->mapLock);
  return out;
}

void replayLaneSetStmt2Shape(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId,
                             int32_t colCount, int32_t tagCount, bool known) {
  (void)taosThreadMutexLock(&sh->mapLock);
  for (int32_t index = 0; index < sh->stmtCount; ++index) {
    SReplayStmtEntry *entry = &sh->stmts[index];
    if (entry->sessionIdx != sessionIdx || entry->recordedId != recordedId || entry->stmt2 == NULL) continue;
    entry->stmt2ColCount = colCount;
    entry->stmt2TagCount = tagCount;
    entry->stmt2ShapeKnown = known;
    break;
  }
  (void)taosThreadMutexUnlock(&sh->mapLock);
}

bool replayLaneGetStmt2Shape(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId,
                             int32_t *colCount, int32_t *tagCount) {
  bool found = false;
  (void)taosThreadMutexLock(&sh->mapLock);
  for (int32_t index = 0; index < sh->stmtCount; ++index) {
    SReplayStmtEntry *entry = &sh->stmts[index];
    if (entry->sessionIdx != sessionIdx || entry->recordedId != recordedId || entry->stmt2 == NULL) continue;
    if (entry->stmt2ShapeKnown) {
      *colCount = entry->stmt2ColCount;
      *tagCount = entry->stmt2TagCount;
      found = true;
    }
    break;
  }
  (void)taosThreadMutexUnlock(&sh->mapLock);
  return found;
}

bool replayLaneAddStmt(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId, uint64_t connId,
                       TAOS_STMT *stmt, TAOS_STMT2 *stmt2) {
  bool full = false;
  bool report = false;
  (void)taosThreadMutexLock(&sh->mapLock);
  if (sh->stmtCount < REPLAY_MAX_HANDLES) {
    sh->stmts[sh->stmtCount].sessionIdx = sessionIdx;
    sh->stmts[sh->stmtCount].recordedId = recordedId;
    sh->stmts[sh->stmtCount].connId = connId;
    sh->stmts[sh->stmtCount].stmt = stmt;
    sh->stmts[sh->stmtCount].stmt2 = stmt2;
    sh->stmts[sh->stmtCount].stmtShapeKnown = false;
    sh->stmts[sh->stmtCount].stmt2ShapeKnown = false;
    sh->stmtCount++;
  } else {
    full = true;
    /* Same reasoning as replayLaneAddConn: report once and close the untracked handle. */
    if (!sh->stmtMapFullReported) {
      sh->stmtMapFullReported = true;
      report = true;
    }
  }
  (void)taosThreadMutexUnlock(&sh->mapLock);
  if (report) {
    (void)fprintf(stderr,
                  "replay: more than %d statements registered; later statements are not "
                  "tracked and their calls will be skipped (session=%d recordedId=%" PRIu64 ")\n",
                  REPLAY_MAX_HANDLES, sessionIdx, recordedId);
  }

  if (full) {
    if (stmt != NULL) (void)taos_stmt_close(stmt);
    if (stmt2 != NULL) (void)taos_stmt2_close(stmt2);
  }
  return !full;
}

void replayLaneDropStmtResults(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId) {
  int32_t i;
  (void)taosThreadMutexLock(&sh->mapLock);
  for (i = 0; i < sh->resultCount; ++i) {
    if (sh->results[i].sessionIdx == sessionIdx && sh->results[i].stmtId == recordedId &&
        !sh->results[i].owned) {
      sh->results[i].retired = true;
    }
  }
  for (;;) {
    bool borrowed = false;
    for (i = 0; i < sh->resultCount; ++i) {
      if (sh->results[i].sessionIdx == sessionIdx && sh->results[i].stmtId == recordedId &&
          !sh->results[i].owned && sh->results[i].borrowers > 0) {
        borrowed = true;
        break;
      }
    }
    if (!borrowed) break;
    (void)taosThreadCondWait(&sh->mapCond, &sh->mapLock);
  }
  for (i = 0; i < sh->resultCount;) {
    if (sh->results[i].sessionIdx == sessionIdx && sh->results[i].stmtId == recordedId &&
        !sh->results[i].owned) {
      sh->results[i] = sh->results[sh->resultCount - 1];
      sh->resultCount--;
      continue;
    }
    ++i;
  }
  (void)taosThreadMutexUnlock(&sh->mapLock);
}

void replayLaneDropStmt(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId) {
  replayLaneDropStmtResults(sh, sessionIdx, recordedId);
  (void)taosThreadMutexLock(&sh->mapLock);
  for (int32_t i = 0; i < sh->stmtCount; ++i) {
    if (sh->stmts[i].sessionIdx == sessionIdx && sh->stmts[i].recordedId == recordedId) {
      sh->stmts[i] = sh->stmts[sh->stmtCount - 1];
      sh->stmtCount--;
      break;
    }
  }
  (void)taosThreadMutexUnlock(&sh->mapLock);
}

/* -------------------------------------------------------------------- stats */

void replayLaneAddStat(SReplayShared *sh, int32_t executed, int32_t skipped, int32_t errors,
                       int32_t mismatches) {
  (void)taosThreadMutexLock(&sh->statLock);
  sh->executed += executed;
  sh->skipped += skipped;
  sh->errors += errors;
  sh->codeMismatches += mismatches;
  (void)taosThreadMutexUnlock(&sh->statLock);
}

void replayLaneNoteLateness(SReplayShared *sh, int64_t lateNs) {
  if (lateNs <= 0) return;
  (void)taosThreadMutexLock(&sh->statLock);
  if (lateNs > sh->maxLatenessNs) sh->maxLatenessNs = lateNs;
  (void)taosThreadMutexUnlock(&sh->statLock);
}

typedef struct {
  uint64_t callId;
  int32_t  sessionIdx;
  int32_t  beginIdx;
  int32_t  terminalIdx;
  bool     used;
} SReplayBeginSlot;

typedef struct {
  uint64_t resultId;
  int32_t  sessionIdx;
  int32_t  firstUseIdx;
  bool     used;
} SReplayResultSlot;

typedef struct {
  uint64_t stmtId;
  uint64_t connId;
  int32_t  sessionIdx;
  bool     used;
} SReplayStmtOwnerSlot;

typedef struct {
  uint64_t stmtId;
  int32_t  sessionIdx;
  int32_t  lastExecIdx;
  int32_t  lastCloseIdx;
  int32_t  lastPrepareIdx;
  int32_t  activeCallbacks;
  int32_t  activeBinds;
  bool     used;
} SReplayStmtExecSlot;

enum {
  REPLAY_ACTIVE_STMT = 0,
  REPLAY_ACTIVE_CONN = 1,
  REPLAY_ACTIVE_OWNER_CONN = 2,
  REPLAY_ACTIVE_LINKS = 3,
};

static size_t replayBeginHash(uint64_t callId, int32_t sessionIdx, size_t mask) {
  uint64_t mixed = callId * REPLAY_BEGIN_HASH_MULTIPLIER ^ (uint64_t)(uint32_t)sessionIdx;
  return (size_t)mixed & mask;
}

static SReplayBeginSlot *replayFindBeginSlot(SReplayBeginSlot *begins, size_t mapCap,
                                              uint64_t callId, int32_t sessionIdx, bool *found) {
  size_t slot = replayBeginHash(callId, sessionIdx, mapCap - 1);
  size_t probes;

  *found = false;
  for (probes = 0; probes < mapCap; ++probes) {
    if (!begins[slot].used) return &begins[slot];
    if (begins[slot].callId == callId && begins[slot].sessionIdx == sessionIdx) {
      *found = true;
      return &begins[slot];
    }
    slot = (slot + 1) & (mapCap - 1);
  }
  return NULL;
}

static SReplayResultSlot *replayFindResultSlot(SReplayResultSlot *results, size_t mapCap,
                                                uint64_t resultId, int32_t sessionIdx, bool *found) {
  size_t slot = replayBeginHash(resultId, sessionIdx, mapCap - 1);
  size_t probes;

  *found = false;
  for (probes = 0; probes < mapCap; ++probes) {
    if (!results[slot].used) return &results[slot];
    if (results[slot].resultId == resultId && results[slot].sessionIdx == sessionIdx) {
      *found = true;
      return &results[slot];
    }
    slot = (slot + 1) & (mapCap - 1);
  }
  return NULL;
}

static SReplayStmtOwnerSlot *replayFindStmtOwnerSlot(SReplayStmtOwnerSlot *owners, size_t mapCap,
                                                      uint64_t stmtId, int32_t sessionIdx, bool *found) {
  size_t slot = replayBeginHash(stmtId, sessionIdx, mapCap - 1);
  size_t probes;

  *found = false;
  for (probes = 0; probes < mapCap; ++probes) {
    if (!owners[slot].used) return &owners[slot];
    if (owners[slot].stmtId == stmtId && owners[slot].sessionIdx == sessionIdx) {
      *found = true;
      return &owners[slot];
    }
    slot = (slot + 1) & (mapCap - 1);
  }
  return NULL;
}

static SReplayStmtExecSlot *replayFindStmtExecSlot(SReplayStmtExecSlot *execs, size_t mapCap,
                                                  uint64_t stmtId, int32_t sessionIdx, bool *found) {
  size_t slot = replayBeginHash(stmtId, sessionIdx, mapCap - 1);
  *found = false;
  for (size_t probes = 0; probes < mapCap; ++probes) {
    if (!execs[slot].used) return &execs[slot];
    if (execs[slot].stmtId == stmtId && execs[slot].sessionIdx == sessionIdx) {
      *found = true;
      return &execs[slot];
    }
    slot = (slot + 1) & (mapCap - 1);
  }
  return NULL;
}

static void replayActiveInsert(int32_t *heads, int32_t *next, int32_t *prev,
                               int32_t *buckets, int32_t bucket, int32_t node) {
  int32_t oldHead = heads[bucket];
  buckets[node] = bucket;
  prev[node] = -1;
  next[node] = oldHead;
  if (oldHead >= 0) prev[oldHead] = node;
  heads[bucket] = node;
}

static void replayActiveRemove(int32_t *heads, int32_t *next, int32_t *prev,
                               int32_t *buckets, int32_t node) {
  int32_t bucket = buckets[node];
  if (bucket < 0) return;
  if (prev[node] >= 0) next[prev[node]] = next[node];
  else heads[bucket] = next[node];
  if (next[node] >= 0) prev[next[node]] = prev[node];
  buckets[node] = -1;
}

static int32_t replayCheckStmt2CallbackDependencies(SReplayShared *sh, SReplayBeginSlot *begins,
                                                    size_t mapCap) {
  int32_t eventTotal = sh->session->eventCount;
  int32_t *callbackHead = (int32_t *)taosMemoryMalloc((size_t)eventTotal * sizeof(int32_t));
  int32_t *callbackNext = (int32_t *)taosMemoryMalloc((size_t)eventTotal * sizeof(int32_t));
  int32_t *callbackParent = (int32_t *)taosMemoryMalloc((size_t)eventTotal * sizeof(int32_t));
  SReplayStmtExecSlot *execs =
      (SReplayStmtExecSlot *)taosMemoryCalloc(mapCap, sizeof(SReplayStmtExecSlot));
  if (callbackHead == NULL || callbackNext == NULL || callbackParent == NULL || execs == NULL) {
    taosMemoryFree(callbackHead);
    taosMemoryFree(callbackNext);
    taosMemoryFree(callbackParent);
    taosMemoryFree(execs);
    return REPLAY_EXIT_INTERNAL;
  }
  for (int32_t index = 0; index < eventTotal; ++index) {
    callbackHead[index] = -1;
    callbackParent[index] = -1;
  }
  int32_t result = REPLAY_EXIT_OK;
  for (int32_t i = 0; i < eventTotal; ++i) {
    const SReplayEvent *child = &sh->session->events[i];
    if (child->phase != WLR_PHASE_BEGIN || child->parentCallId == 0) continue;
    bool found;
    SReplayBeginSlot *parent = replayFindBeginSlot(begins, mapCap, child->parentCallId,
                                                  child->sessionIdx, &found);
    if (parent == NULL || !found) continue;
    if (parent->beginIdx >= i) {
      result = REPLAY_EXIT_INPUT;
      break;
    }
    int32_t callbackIdx = callbackParent[parent->beginIdx];
    const SReplayEvent *ancestor = &sh->session->events[parent->beginIdx];
    if (ancestor->apiType == WLR_API_STMT2_BIND_PARAM_A ||
        ancestor->apiType == WLR_API_STMT2_BIND_PARAM_COLUMN_A ||
        (ancestor->apiType == WLR_API_STMT2_EXEC && ancestor->stmt2Async)) {
      if (callbackIdx >= 0) {
        result = REPLAY_EXIT_INPUT;
        break;
      }
      callbackIdx = parent->beginIdx;
    }
    if (callbackIdx < 0) continue;
    int32_t terminalIdx = sh->terminalByBegin[i];
    if (terminalIdx < 0) terminalIdx = eventTotal - 1;
    callbackParent[i] = callbackIdx;
    callbackNext[i] = callbackHead[terminalIdx];
    callbackHead[terminalIdx] = i;
  }

  for (int32_t i = 0; i < eventTotal && result == REPLAY_EXIT_OK; ++i) {
    const SReplayEvent *event = &sh->session->events[i];
    if (event->phase == WLR_PHASE_BEGIN &&
        (event->apiType == WLR_API_STMT2_EXEC || event->apiType == WLR_API_STMT2_CLOSE ||
         event->apiType == WLR_API_STMT2_PREPARE) &&
        event->stmtId != 0) {
      bool found;
      SReplayStmtExecSlot *slot = replayFindStmtExecSlot(execs, mapCap, event->stmtId,
                                                        event->sessionIdx, &found);
      if (slot == NULL) {
        result = REPLAY_EXIT_INTERNAL;
        break;
      }
      slot->used = true;
      slot->stmtId = event->stmtId;
      slot->sessionIdx = event->sessionIdx;
      if (event->apiType == WLR_API_STMT2_EXEC) slot->lastExecIdx = i;
      else if (event->apiType == WLR_API_STMT2_CLOSE) slot->lastCloseIdx = i;
      else slot->lastPrepareIdx = i;
    }
    for (int32_t childIdx = callbackHead[i]; childIdx >= 0; childIdx = callbackNext[childIdx]) {
      const SReplayEvent *callbackCall = &sh->session->events[callbackParent[childIdx]];
      bool found;
      SReplayStmtExecSlot *slot = replayFindStmtExecSlot(execs, mapCap, callbackCall->stmtId,
                                                        callbackCall->sessionIdx, &found);
      if (slot != NULL && found && slot->lastCloseIdx > callbackParent[childIdx]) {
        (void)fprintf(stderr,
                      "replay: STMT2 callback overlaps close on statement id=%" PRIu64
                      "; refusing unreplayable callback dependency\n", callbackCall->stmtId);
        result = REPLAY_EXIT_INPUT;
        break;
      }
      if (slot != NULL && found && slot->lastPrepareIdx > callbackParent[childIdx]) {
        (void)fprintf(stderr,
                      "replay: STMT2 callback overlaps prepare on statement id=%" PRIu64
                      "; refusing unreplayable callback dependency\n", callbackCall->stmtId);
        result = REPLAY_EXIT_INPUT;
        break;
      }
      if (slot != NULL && found && callbackCall->apiType != WLR_API_STMT2_EXEC &&
          slot->lastExecIdx > callbackParent[childIdx]) {
        (void)fprintf(stderr,
                      "replay: STMT2 async bind callback overlaps execute on statement id=%" PRIu64
                      "; refusing unreplayable callback dependency\n", callbackCall->stmtId);
        result = REPLAY_EXIT_INPUT;
        break;
      }
    }
  }

  /* A replayed async EXEC runs synchronously and cannot reproduce database calls made
   * from its user callback. Build callback and bind intervals, then sweep them once so
   * either interval order is rejected without rescanning the complete event history. */
  int32_t *callbackStartHead = (int32_t *)taosMemoryMalloc((size_t)eventTotal * sizeof(int32_t));
  int32_t *callbackEndHead = (int32_t *)taosMemoryMalloc((size_t)eventTotal * sizeof(int32_t));
  int32_t *callbackStartNext = (int32_t *)taosMemoryMalloc((size_t)eventTotal * sizeof(int32_t));
  int32_t *callbackEndNext = (int32_t *)taosMemoryMalloc((size_t)eventTotal * sizeof(int32_t));
  int32_t *bindStartHead = (int32_t *)taosMemoryMalloc((size_t)eventTotal * sizeof(int32_t));
  int32_t *bindEndHead = (int32_t *)taosMemoryMalloc((size_t)eventTotal * sizeof(int32_t));
  int32_t *bindStartNext = (int32_t *)taosMemoryMalloc((size_t)eventTotal * sizeof(int32_t));
  int32_t *bindEndNext = (int32_t *)taosMemoryMalloc((size_t)eventTotal * sizeof(int32_t));
  if (callbackStartHead == NULL || callbackEndHead == NULL || callbackStartNext == NULL ||
      callbackEndNext == NULL || bindStartHead == NULL || bindEndHead == NULL ||
      bindStartNext == NULL || bindEndNext == NULL) {
    result = REPLAY_EXIT_INTERNAL;
  } else {
    for (int32_t index = 0; index < eventTotal; ++index) {
      callbackStartHead[index] = -1;
      callbackEndHead[index] = -1;
      bindStartHead[index] = -1;
      bindEndHead[index] = -1;
    }
    for (int32_t callbackIdx = 0; callbackIdx < eventTotal; ++callbackIdx) {
      int32_t rootIdx = callbackParent[callbackIdx];
      if (rootIdx < 0 || sh->session->events[rootIdx].apiType != WLR_API_STMT2_EXEC ||
          !sh->session->events[rootIdx].stmt2Async) {
        continue;
      }
      int32_t callbackEnd = sh->terminalByBegin[callbackIdx];
      if (callbackEnd >= 0 && callbackEnd <= callbackIdx) continue;
      callbackStartNext[callbackIdx] = callbackStartHead[callbackIdx];
      callbackStartHead[callbackIdx] = callbackIdx;
      if (callbackEnd >= 0) {
        callbackEndNext[callbackIdx] = callbackEndHead[callbackEnd];
        callbackEndHead[callbackEnd] = callbackIdx;
      }
    }
    for (int32_t bindIdx = 0; bindIdx < eventTotal; ++bindIdx) {
      const SReplayEvent *bindEvent = &sh->session->events[bindIdx];
      bool isBind = bindEvent->phase == WLR_PHASE_BEGIN &&
                    (bindEvent->apiType == WLR_API_STMT2_BIND_PARAM ||
                     bindEvent->apiType == WLR_API_STMT2_BIND_PARAM_A ||
                     bindEvent->apiType == WLR_API_STMT2_BIND_PARAM_COLUMN ||
                     bindEvent->apiType == WLR_API_STMT2_BIND_PARAM_COLUMN_A);
      if (!isBind || bindEvent->stmtId == 0) continue;
      int32_t bindEnd = sh->terminalByBegin[bindIdx];
      if (bindEnd >= 0 && bindEnd <= bindIdx) continue;
      bool found;
      SReplayStmtExecSlot *slot = replayFindStmtExecSlot(execs, mapCap, bindEvent->stmtId,
                                                        bindEvent->sessionIdx, &found);
      if (slot == NULL) {
        result = REPLAY_EXIT_INTERNAL;
        break;
      }
      if (!found) {
        slot->used = true;
        slot->stmtId = bindEvent->stmtId;
        slot->sessionIdx = bindEvent->sessionIdx;
        slot->lastExecIdx = -1;
        slot->lastCloseIdx = -1;
        slot->lastPrepareIdx = -1;
      }
      bindStartNext[bindIdx] = bindStartHead[bindIdx];
      bindStartHead[bindIdx] = bindIdx;
      if (bindEnd >= 0) {
        bindEndNext[bindIdx] = bindEndHead[bindEnd];
        bindEndHead[bindEnd] = bindIdx;
      }
    }
    for (int32_t index = 0; index < eventTotal && result == REPLAY_EXIT_OK; ++index) {
      for (int32_t bindIdx = bindEndHead[index]; bindIdx >= 0; bindIdx = bindEndNext[bindIdx]) {
        bool found;
        SReplayEvent *bindEvent = &sh->session->events[bindIdx];
        SReplayStmtExecSlot *slot = replayFindStmtExecSlot(execs, mapCap, bindEvent->stmtId,
                                                          bindEvent->sessionIdx, &found);
        if (slot != NULL && found && slot->activeBinds > 0) slot->activeBinds--;
      }
      for (int32_t callbackIdx = callbackEndHead[index]; callbackIdx >= 0;
           callbackIdx = callbackEndNext[callbackIdx]) {
        const SReplayEvent *root = &sh->session->events[callbackParent[callbackIdx]];
        bool found;
        SReplayStmtExecSlot *slot = replayFindStmtExecSlot(execs, mapCap, root->stmtId,
                                                          root->sessionIdx, &found);
        if (slot != NULL && found && slot->activeCallbacks > 0) slot->activeCallbacks--;
      }
      for (int32_t callbackIdx = callbackStartHead[index]; callbackIdx >= 0;
           callbackIdx = callbackStartNext[callbackIdx]) {
        const SReplayEvent *root = &sh->session->events[callbackParent[callbackIdx]];
        bool found;
        SReplayStmtExecSlot *slot = replayFindStmtExecSlot(execs, mapCap, root->stmtId,
                                                          root->sessionIdx, &found);
        if (slot != NULL && found) {
          if (slot->activeBinds > 0) {
            (void)fprintf(stderr,
                          "replay: STMT2 bind overlaps execute callback on statement id=%" PRIu64
                          "; refusing unreplayable callback dependency\n",
                          root->stmtId);
            result = REPLAY_EXIT_INPUT;
            break;
          }
          slot->activeCallbacks++;
        }
      }
      if (result != REPLAY_EXIT_OK) break;
      for (int32_t bindIdx = bindStartHead[index]; bindIdx >= 0; bindIdx = bindStartNext[bindIdx]) {
        SReplayEvent *bindEvent = &sh->session->events[bindIdx];
        bool found;
        SReplayStmtExecSlot *slot = replayFindStmtExecSlot(execs, mapCap, bindEvent->stmtId,
                                                          bindEvent->sessionIdx, &found);
        if (slot != NULL && found) {
          if (slot->activeCallbacks > 0) {
            (void)fprintf(stderr,
                          "replay: STMT2 bind overlaps execute callback on statement id=%" PRIu64
                          "; refusing unreplayable callback dependency\n",
                          bindEvent->stmtId);
            result = REPLAY_EXIT_INPUT;
            break;
          }
          slot->activeBinds++;
        }
      }
    }
  }
  taosMemoryFree(callbackStartHead);
  taosMemoryFree(callbackEndHead);
  taosMemoryFree(callbackStartNext);
  taosMemoryFree(callbackEndNext);
  taosMemoryFree(bindStartHead);
  taosMemoryFree(bindEndHead);
  taosMemoryFree(bindStartNext);
  taosMemoryFree(bindEndNext);
  taosMemoryFree(callbackHead);
  taosMemoryFree(callbackNext);
  taosMemoryFree(callbackParent);
  taosMemoryFree(execs);
  return result;
}

static bool replayApiUsesResult(uint16_t apiType) {
  switch (apiType) {
    case WLR_API_STOP_QUERY:
    case WLR_API_FETCH_ROW:
    case WLR_API_FETCH_BLOCK:
    case WLR_API_FETCH_BLOCK_S:
    case WLR_API_FETCH_RAW_BLOCK:
    case WLR_API_FETCH_ROWS_A:
    case WLR_API_FETCH_RAW_BLOCK_A:
    case WLR_API_FREE_RESULT:
      return true;
    default:
      return false;
  }
}

int32_t replayLaneInitDeps(SReplayShared *sh) {
  int32_t eventTotal = sh->session->eventCount;
  size_t  mapCap = 1;
  int32_t i;

  /* Init is repeatable on the zero-initialized shared object: discard any arrays
   * from a previous successful call before an overflow check can return early. */
  replayLaneFreeDeps(sh);
  if (eventTotal <= 0) return REPLAY_EXIT_OK;

  if ((size_t)eventTotal > SIZE_MAX / 2u) return REPLAY_EXIT_INTERNAL;
  size_t requiredMapCap = (size_t)eventTotal * 2u;
  while (mapCap < requiredMapCap) {
    if (mapCap > SIZE_MAX / 2u) return REPLAY_EXIT_INTERNAL;
    mapCap <<= 1;
  }
  if (mapCap > SIZE_MAX / sizeof(SReplayBeginSlot)) return REPLAY_EXIT_INTERNAL;

  sh->waitForReturnRank =
      (int32_t *)taosMemoryMalloc((size_t)eventTotal * sizeof(int32_t));
  sh->returnRankByEvent =
      (int32_t *)taosMemoryMalloc((size_t)eventTotal * sizeof(int32_t));
  sh->terminalByBegin = (int32_t *)taosMemoryMalloc((size_t)eventTotal * sizeof(int32_t));
  sh->resultUsedByEvent = (bool *)taosMemoryCalloc((size_t)eventTotal, sizeof(bool));
  sh->completedReturns = (bool *)taosMemoryCalloc((size_t)eventTotal, sizeof(bool));
  sh->closePending = (int32_t *)taosMemoryCalloc((size_t)eventTotal, sizeof(int32_t));
  sh->dependentHead = (int32_t *)taosMemoryMalloc((size_t)eventTotal * sizeof(int32_t));
  sh->dependentCapacity = eventTotal <= INT32_MAX / 2 ? eventTotal * 2 : 0;
  sh->dependentNext = sh->dependentCapacity > 0 ?
      (int32_t *)taosMemoryMalloc((size_t)sh->dependentCapacity * sizeof(int32_t)) : NULL;
  sh->dependentClose = sh->dependentCapacity > 0 ?
      (int32_t *)taosMemoryMalloc((size_t)sh->dependentCapacity * sizeof(int32_t)) : NULL;
  SReplayBeginSlot *begins =
      (SReplayBeginSlot *)taosMemoryCalloc(mapCap, sizeof(SReplayBeginSlot));
  int32_t *beginByTerminal = (int32_t *)taosMemoryMalloc((size_t)eventTotal * sizeof(int32_t));
  if (mapCap > (SIZE_MAX / sizeof(int32_t) - (size_t)eventTotal * REPLAY_ACTIVE_LINKS * 3u) / 2u) {
    taosMemoryFree(begins);
    taosMemoryFree(beginByTerminal);
    replayLaneFreeDeps(sh);
    return REPLAY_EXIT_INTERNAL;
  }
  size_t activeLinkCount = (size_t)eventTotal * REPLAY_ACTIVE_LINKS;
  int32_t *activeIndex = (int32_t *)taosMemoryMalloc((mapCap * 2u + activeLinkCount * 3u) * sizeof(int32_t));
  int32_t *activeHeads = activeIndex;
  int32_t *activeNext = activeIndex != NULL ? activeIndex + mapCap * 2u : NULL;
  int32_t *activePrev = activeNext != NULL ? activeNext + activeLinkCount : NULL;
  int32_t *activeBucket = activePrev != NULL ? activePrev + activeLinkCount : NULL;
  if (sh->waitForReturnRank == NULL || sh->returnRankByEvent == NULL || sh->terminalByBegin == NULL ||
      sh->resultUsedByEvent == NULL ||
      sh->completedReturns == NULL || sh->closePending == NULL || sh->dependentHead == NULL ||
      sh->dependentNext == NULL || sh->dependentClose == NULL || begins == NULL ||
      beginByTerminal == NULL || activeIndex == NULL) {
    taosMemoryFree(begins);
    taosMemoryFree(beginByTerminal);
    taosMemoryFree(activeIndex);
    replayLaneFreeDeps(sh);
    return REPLAY_EXIT_INTERNAL;
  }
  for (size_t slot = 0; slot < mapCap * 2u; ++slot) activeHeads[slot] = -1;
  for (size_t node = 0; node < activeLinkCount; ++node) activeBucket[node] = -1;

  for (i = 0; i < eventTotal; ++i) {
    sh->waitForReturnRank[i] = -1;
    sh->returnRankByEvent[i] = -1;
    sh->terminalByBegin[i] = -1;
    sh->dependentHead[i] = -1;
    beginByTerminal[i] = -1;
    const SReplayEvent *event = &sh->session->events[i];
    if (event->phase != WLR_PHASE_BEGIN || event->callId == 0) continue;
    bool found;
    SReplayBeginSlot *begin = replayFindBeginSlot(begins, mapCap, event->callId,
                                                  event->sessionIdx, &found);
    if (begin == NULL) {
      (void)fprintf(stderr,
                    "replay: dependency BEGIN table exhausted at event=%d callId=%" PRIu64
                    " session=%d\n",
                    i, event->callId, event->sessionIdx);
      taosMemoryFree(begins);
      replayLaneFreeDeps(sh);
      return REPLAY_EXIT_INTERNAL;
    }
    if (found) {
      (void)fprintf(stderr, "replay: duplicate BEGIN callId=%" PRIu64 " session=%d\n",
                    event->callId, event->sessionIdx);
      taosMemoryFree(begins);
      replayLaneFreeDeps(sh);
      return REPLAY_EXIT_INPUT;
    }
    begin->used = true;
    begin->callId = event->callId;
    begin->sessionIdx = event->sessionIdx;
    begin->beginIdx = i;
    begin->terminalIdx = -1;
  }

  /* Async calls carry both RETURN (dispatch returned) and DONE (operation
   * finished). replayPhaseTerminatesCall is shared with unfinished-call counting
   * so dependency release and watchdog activation use the same terminal rule. */
  for (i = 0; i < eventTotal; ++i) {
    const SReplayEvent *event = &sh->session->events[i];
    if (event->phase == WLR_PHASE_BEGIN || event->callId == 0) continue;
    bool found;
    SReplayBeginSlot *begin = replayFindBeginSlot(begins, mapCap, event->callId,
                                                  event->sessionIdx, &found);
    if (begin == NULL) {
      (void)fprintf(stderr,
                    "replay: dependency table exhausted while matching event=%d callId=%" PRIu64
                    " session=%d\n",
                    i, event->callId, event->sessionIdx);
      taosMemoryFree(begins);
      replayLaneFreeDeps(sh);
      return REPLAY_EXIT_INTERNAL;
    }
    if (found) {
      const SReplayEvent *beginEvent = &sh->session->events[begin->beginIdx];
      if (replayPhaseTerminatesCall(beginEvent->apiType, beginEvent->stmt2Async, event->phase) &&
          (event->phase == WLR_PHASE_DONE || begin->terminalIdx < 0)) {
        begin->terminalIdx = i;
      }
    }
  }

  int32_t returnRank = 0;
  for (i = 0; i < eventTotal; ++i) {
    const SReplayEvent *event = &sh->session->events[i];
    if (event->phase == WLR_PHASE_BEGIN) {
      sh->waitForReturnRank[i] = returnRank - 1;
      continue;
    }
    if (event->callId == 0) continue;
    bool found;
    SReplayBeginSlot *begin = replayFindBeginSlot(begins, mapCap, event->callId,
                                                  event->sessionIdx, &found);
    if (begin == NULL) {
      (void)fprintf(stderr,
                    "replay: dependency table exhausted while ranking event=%d callId=%" PRIu64
                    " session=%d\n",
                    i, event->callId, event->sessionIdx);
      taosMemoryFree(begins);
      replayLaneFreeDeps(sh);
      return REPLAY_EXIT_INTERNAL;
    }
    if (found) {
      int32_t beginIdx = begin->beginIdx;
      sh->terminalByBegin[beginIdx] = begin->terminalIdx;
      if (begin->terminalIdx == i && sh->returnRankByEvent[beginIdx] < 0) {
        sh->returnRankByEvent[beginIdx] = returnRank++;
        beginByTerminal[i] = beginIdx;
      }
    }
  }
  sh->eventTotal = eventTotal;
  sh->returnCount = returnRank;

  /* Materialize handle-lifetime dependencies once. Runtime wait predicates then
   * read one counter instead of rescanning the complete event history under depLock. */
  SReplayStmtOwnerSlot *stmtOwners =
      (SReplayStmtOwnerSlot *)taosMemoryCalloc(mapCap, sizeof(SReplayStmtOwnerSlot));
  if (stmtOwners == NULL) {
    taosMemoryFree(begins);
    taosMemoryFree(beginByTerminal);
    taosMemoryFree(activeIndex);
    replayLaneFreeDeps(sh);
    return REPLAY_EXIT_INTERNAL;
  }
  for (i = 0; i < eventTotal; ++i) {
    const SReplayEvent *event = &sh->session->events[i];
    if (event->phase != WLR_PHASE_BEGIN || event->stmtId == 0 || event->connId == 0 ||
        (event->apiType != WLR_API_STMT_INIT && event->apiType != WLR_API_STMT_INIT_WITH_REQID &&
         event->apiType != WLR_API_STMT_INIT_WITH_OPTIONS && event->apiType != WLR_API_STMT2_INIT)) {
      continue;
    }
    bool found;
    SReplayStmtOwnerSlot *owner = replayFindStmtOwnerSlot(stmtOwners, mapCap, event->stmtId,
                                                          event->sessionIdx, &found);
    if (owner == NULL) {
      taosMemoryFree(stmtOwners);
      taosMemoryFree(begins);
      taosMemoryFree(beginByTerminal);
      taosMemoryFree(activeIndex);
      replayLaneFreeDeps(sh);
      return REPLAY_EXIT_INTERNAL;
    }
    if (found && owner->connId != event->connId) {
      (void)fprintf(stderr,
                    "replay: statement id=%" PRIu64 " session=%d has multiple owning connections\n",
                    event->stmtId, event->sessionIdx);
      taosMemoryFree(stmtOwners);
      taosMemoryFree(begins);
      taosMemoryFree(beginByTerminal);
      taosMemoryFree(activeIndex);
      replayLaneFreeDeps(sh);
      return REPLAY_EXIT_INPUT;
    }
    owner->used = true;
    owner->stmtId = event->stmtId;
    owner->connId = event->connId;
    owner->sessionIdx = event->sessionIdx;
  }

  for (i = 0; i < eventTotal; ++i) {
    const SReplayEvent *current = &sh->session->events[i];
    bool bindAfterExec = current->apiType == WLR_API_STMT2_BIND_PARAM ||
                         current->apiType == WLR_API_STMT2_BIND_PARAM_A ||
                         current->apiType == WLR_API_STMT2_BIND_PARAM_COLUMN ||
                         current->apiType == WLR_API_STMT2_BIND_PARAM_COLUMN_A;
    bool prepareAfterExec = current->apiType == WLR_API_STMT2_PREPARE;
    if (current->phase == WLR_PHASE_BEGIN &&
        (current->apiType == WLR_API_CLOSE || current->apiType == WLR_API_STMT_CLOSE ||
         current->apiType == WLR_API_STMT2_CLOSE || current->apiType == WLR_API_STMT2_EXEC ||
         bindAfterExec || prepareAfterExec)) {
      bool byConnection = current->apiType == WLR_API_CLOSE;
      uint64_t handleId = byConnection ? current->connId : current->stmtId;
      int32_t bucket = handleId != 0
                           ? (int32_t)replayBeginHash(handleId, current->sessionIdx, mapCap - 1) +
                                 (byConnection ? (int32_t)mapCap : 0)
                           : -1;
      for (int32_t node = bucket >= 0 ? activeHeads[bucket] : -1; node >= 0;
           node = activeNext[node]) {
        int32_t priorIdx = node / REPLAY_ACTIVE_LINKS;
        const SReplayEvent *prior = &sh->session->events[priorIdx];
        if (prior->sessionIdx != current->sessionIdx) continue;
        bool same = byConnection
                        ? (node % REPLAY_ACTIVE_LINKS == REPLAY_ACTIVE_CONN && prior->connId == current->connId)
                        : prior->stmtId == current->stmtId;
        if (byConnection && node % REPLAY_ACTIVE_LINKS == REPLAY_ACTIVE_OWNER_CONN && prior->stmtId != 0) {
          bool found;
          SReplayStmtOwnerSlot *owner = replayFindStmtOwnerSlot(stmtOwners, mapCap, prior->stmtId,
                                                                prior->sessionIdx, &found);
          same = owner != NULL && found && owner->connId == current->connId;
        }
        if (current->apiType == WLR_API_STMT2_EXEC) {
          same = same && (prior->apiType == WLR_API_STMT2_BIND_PARAM_A ||
                          prior->apiType == WLR_API_STMT2_BIND_PARAM_COLUMN_A);
        } else if (bindAfterExec || prepareAfterExec) {
          same = same && prior->apiType == WLR_API_STMT2_EXEC && prior->stmt2Async;
        }
        if (!same) continue;
        if (sh->dependentCount >= sh->dependentCapacity) {
          int32_t newCapacity = sh->dependentCapacity <= INT32_MAX / 2 ? sh->dependentCapacity * 2 : INT32_MAX;
          if (newCapacity <= sh->dependentCapacity ||
              (size_t)newCapacity > SIZE_MAX / sizeof(int32_t)) {
            taosMemoryFree(stmtOwners);
            taosMemoryFree(begins);
            taosMemoryFree(beginByTerminal);
            taosMemoryFree(activeIndex);
            replayLaneFreeDeps(sh);
            return REPLAY_EXIT_INTERNAL;
          }
          int32_t *next = (int32_t *)taosMemoryRealloc(sh->dependentNext, (size_t)newCapacity * sizeof(int32_t));
          if (next != NULL) sh->dependentNext = next;
          int32_t *target = next != NULL
                                ? (int32_t *)taosMemoryRealloc(sh->dependentClose,
                                                               (size_t)newCapacity * sizeof(int32_t))
                                : NULL;
          if (target == NULL) {
            taosMemoryFree(stmtOwners);
            taosMemoryFree(begins);
            taosMemoryFree(beginByTerminal);
            taosMemoryFree(activeIndex);
            replayLaneFreeDeps(sh);
            return REPLAY_EXIT_INTERNAL;
          }
          sh->dependentClose = target;
          sh->dependentCapacity = newCapacity;
        }
        int32_t edge = sh->dependentCount++;
        sh->dependentClose[edge] = i;
        sh->dependentNext[edge] = sh->dependentHead[priorIdx];
        sh->dependentHead[priorIdx] = edge;
        sh->closePending[i]++;
      }
    }
    if (current->phase == WLR_PHASE_BEGIN) {
      int32_t node = i * REPLAY_ACTIVE_LINKS;
      if (current->stmtId != 0) {
        int32_t bucket = (int32_t)replayBeginHash(current->stmtId, current->sessionIdx, mapCap - 1);
        replayActiveInsert(activeHeads, activeNext, activePrev, activeBucket, bucket, node + REPLAY_ACTIVE_STMT);
      }
      if (current->connId != 0) {
        int32_t bucket = (int32_t)replayBeginHash(current->connId, current->sessionIdx, mapCap - 1) +
                         (int32_t)mapCap;
        replayActiveInsert(activeHeads, activeNext, activePrev, activeBucket, bucket, node + REPLAY_ACTIVE_CONN);
      }
      if (current->stmtId != 0) {
        bool found;
        SReplayStmtOwnerSlot *owner = replayFindStmtOwnerSlot(stmtOwners, mapCap, current->stmtId,
                                                              current->sessionIdx, &found);
        if (owner != NULL && found && owner->connId != 0 && owner->connId != current->connId) {
          int32_t bucket = (int32_t)replayBeginHash(owner->connId, current->sessionIdx, mapCap - 1) +
                           (int32_t)mapCap;
          replayActiveInsert(activeHeads, activeNext, activePrev, activeBucket, bucket,
                             node + REPLAY_ACTIVE_OWNER_CONN);
        }
      }
    }
    if (beginByTerminal[i] >= 0) {
      const SReplayEvent *completed = &sh->session->events[beginByTerminal[i]];
      bool found;
      SReplayBeginSlot *begin = replayFindBeginSlot(begins, mapCap, completed->callId,
                                                    completed->sessionIdx, &found);
      if (found) {
        int32_t node = begin->beginIdx * REPLAY_ACTIVE_LINKS;
        for (int32_t link = 0; link < REPLAY_ACTIVE_LINKS; ++link) {
          replayActiveRemove(activeHeads, activeNext, activePrev, activeBucket, node + link);
        }
      }
    }
  }
  taosMemoryFree(stmtOwners);

  SReplayResultSlot *resultUses =
      (SReplayResultSlot *)taosMemoryCalloc(mapCap, sizeof(SReplayResultSlot));
  if (resultUses == NULL) {
    taosMemoryFree(begins);
    taosMemoryFree(beginByTerminal);
    taosMemoryFree(activeIndex);
    replayLaneFreeDeps(sh);
    return REPLAY_EXIT_INTERNAL;
  }
  for (i = 0; i < eventTotal; ++i) {
    const SReplayEvent *event = &sh->session->events[i];
    if (event->phase != WLR_PHASE_BEGIN || event->resultId == 0 ||
        !replayApiUsesResult(event->apiType)) {
      continue;
    }
    bool found;
    SReplayResultSlot *slot = replayFindResultSlot(resultUses, mapCap, event->resultId,
                                                    event->sessionIdx, &found);
    if (slot == NULL) {
      taosMemoryFree(resultUses);
      taosMemoryFree(begins);
      taosMemoryFree(beginByTerminal);
      taosMemoryFree(activeIndex);
      replayLaneFreeDeps(sh);
      return REPLAY_EXIT_INTERNAL;
    }
    if (!found) {
      slot->used = true;
      slot->resultId = event->resultId;
      slot->sessionIdx = event->sessionIdx;
      slot->firstUseIdx = i;
    }
  }
  for (i = 0; i < eventTotal; ++i) {
    const SReplayEvent *event = &sh->session->events[i];
    if (event->resultId == 0) continue;
    bool found;
    SReplayResultSlot *slot =
        replayFindResultSlot(resultUses, mapCap, event->resultId, event->sessionIdx, &found);
    sh->resultUsedByEvent[i] = found && slot != NULL && i < slot->firstUseIdx;
  }
  taosMemoryFree(resultUses);

  int32_t callbackCode = replayCheckStmt2CallbackDependencies(sh, begins, mapCap);
  if (callbackCode != REPLAY_EXIT_OK) {
    taosMemoryFree(beginByTerminal);
    taosMemoryFree(activeIndex);
    taosMemoryFree(begins);
    replayLaneFreeDeps(sh);
    return callbackCode;
  }
  taosMemoryFree(beginByTerminal);
  taosMemoryFree(activeIndex);
  taosMemoryFree(begins);
  return REPLAY_EXIT_OK;
}

void replayLaneFreeDeps(SReplayShared *sh) {
  taosMemoryFreeClear(sh->waitForReturnRank);
  taosMemoryFreeClear(sh->returnRankByEvent);
  taosMemoryFreeClear(sh->terminalByBegin);
  taosMemoryFreeClear(sh->resultUsedByEvent);
  taosMemoryFreeClear(sh->completedReturns);
  taosMemoryFreeClear(sh->closePending);
  taosMemoryFreeClear(sh->dependentHead);
  taosMemoryFreeClear(sh->dependentNext);
  taosMemoryFreeClear(sh->dependentClose);
  sh->dependentCount = 0;
  sh->dependentCapacity = 0;
  sh->eventTotal = 0;
  sh->returnCount = 0;
  sh->completedReturnUpto = -1;
}

/* ------------------------------------------------------------------- timing */

static void replayLaneWarnPacing(SReplayShared *sh, const char *message) {
  bool report = false;
  (void)taosThreadMutexLock(&sh->statLock);
  if (!sh->pacingWarned) {
    sh->pacingWarned = true;
    report = true;
  }
  (void)taosThreadMutexUnlock(&sh->statLock);
  if (report) (void)fprintf(stderr, "replay: %s; executing without pacing\n", message);
}

void replayLaneWaitUntil(SReplayShared *sh, int64_t eventMonoNs) {
  /*
   * Absolute target, computed from the shared origin rather than by accumulating
   * per-call sleeps: with relative delays every lane's clock would drift by its own
   * execution time, and the relative arrival order across lanes -- the thing being
   * reproduced -- would degrade over a long replay (DS 6.19).
   */
  if (eventMonoNs == INT64_MAX || sh->originMonoNs < 0) return;
  if (eventMonoNs < sh->originMonoNs) {
    replayLaneWarnPacing(sh, "event monotonic timestamp precedes the session origin");
    return;
  }
  uint64_t offset = (uint64_t)eventMonoNs - (uint64_t)sh->originMonoNs;
  if (offset > (uint64_t)INT64_MAX ||
      (sh->startMonoNs >= 0 && (uint64_t)sh->startMonoNs > (uint64_t)INT64_MAX - offset)) {
    replayLaneWarnPacing(sh, "event pacing target is outside the monotonic clock range");
    return;
  }
  int64_t target = sh->startMonoNs + (int64_t)offset;
  int64_t now = taosGetMonoTimestampNs();

  while (target > now) {
    if (replayLaneAborted(sh)) return;
    int64_t waitNs = target - now;
    /* The 100 ms cap bounds abort latency; every iteration recomputes the same
     * absolute target, so chunking does not accumulate drift or add 100 ms of timing
     * error. The sub-millisecond path also rechecks after an early/EINTR wakeup. */
    int64_t waitMs = waitNs / 1000000;
    if (waitMs > 0) {
      const int32_t maxChunkMs = 100;
      int32_t waitChunkMs = waitMs > maxChunkMs ? maxChunkMs : (int32_t)waitMs;
      taosMsleep(waitChunkMs);
    } else {
      /* The loop condition and freshly computed difference guarantee waitNs >= 1;
       * truncation may produce waitUs==0 only for a sub-microsecond final spin. */
      int32_t waitUs = (int32_t)(waitNs / 1000);
      taosUsleep(waitUs > 0 ? waitUs : 1);
    }
    now = taosGetMonoTimestampNs();
  }
  replayLaneNoteLateness(sh, now - target);
}

/* -------------------------------------------------------------- dependencies */

/*
 * Wait until calls observed to return before this one began have completed.
 *
 * A recorded RETURN/DONE establishes a happens-before boundary without requiring SQL
 * semantics. Calls still in flight at this BEGIN are deliberately excluded, so their
 * lanes remain concurrent instead of being serialized by event index.
 */
static bool replayDepsSatisfied(SReplayShared *sh, int32_t selfIdx) {
  if (selfIdx < 0 || selfIdx >= sh->eventTotal ||
      sh->completedReturnUpto < sh->waitForReturnRank[selfIdx]) return false;
  return sh->closePending[selfIdx] == 0;
}

void replayLaneWaitDeps(SReplayShared *sh, int32_t selfIdx) {
  (void)taosThreadMutexLock(&sh->depLock);
  if (selfIdx < 0 || selfIdx >= sh->eventTotal || sh->waitForReturnRank == NULL ||
      sh->returnRankByEvent == NULL || sh->completedReturns == NULL || sh->closePending == NULL) {
    (void)taosThreadMutexUnlock(&sh->depLock);
    replayLaneAbort(sh, REPLAY_EXIT_INTERNAL);
    (void)fprintf(stderr, "replay: invalid dependency index %d\n", selfIdx);
    return;
  }
  while (!replayDepsSatisfied(sh, selfIdx) && sh->aborted == 0) {
    /*
     * Unbounded wait, released by a depCond broadcast from replayLaneMarkDone or
     * replayLaneAbort. It is safe only because every index this predicate can block on
     * is guaranteed to be published by its BEGIN lane. A skipped BEGIN is explicitly
     * marked done during partitioning, and a lane-spawn failure aborts every waiter.
     */
    (void)taosThreadCondWait(&sh->depCond, &sh->depLock);
  }
  (void)taosThreadMutexUnlock(&sh->depLock);
}

void replayLaneAbort(SReplayShared *sh, int32_t exitCode) {
  (void)taosThreadMutexLock(&sh->depLock);
  /* First writer wins: the lane that failed first has the most relevant reason. */
  if (sh->aborted == 0) {
    sh->aborted = 1;
    sh->abortCode = exitCode;
  }
  /* Broadcast, not signal: every waiting lane must be released, not just one. */
  (void)taosThreadCondBroadcast(&sh->depCond);
  (void)taosThreadMutexUnlock(&sh->depLock);

  /* Handle-lifetime waits use mapCond, not depCond. Mirror the abort under mapLock
   * after dropping depLock so the two lock domains never acquire in opposite order. */
  (void)taosThreadMutexLock(&sh->mapLock);
  sh->mapAborted = true;
  (void)taosThreadCondBroadcast(&sh->mapCond);
  (void)taosThreadMutexUnlock(&sh->mapLock);
}

/*
 * Read under depLock, not as a bare load. aborted is written under that lock, whose
 * acquire/release ordering ensures a poller cannot observe a stale 0 and keep
 * dispatching after another lane has already failed.
 * Contention is irrelevant here: this runs once per event, not per lookup.
 */
bool replayLaneAborted(SReplayShared *sh) {
  bool v;
  (void)taosThreadMutexLock(&sh->depLock);
  v = (sh->aborted != 0);
  (void)taosThreadMutexUnlock(&sh->depLock);
  return v;
}

void replayLaneMarkDone(SReplayShared *sh, int32_t selfIdx) {
  (void)taosThreadMutexLock(&sh->depLock);
  if (sh->waitForReturnRank == NULL || sh->returnRankByEvent == NULL ||
      sh->completedReturns == NULL || sh->closePending == NULL || sh->dependentHead == NULL || selfIdx < 0 ||
      selfIdx >= sh->eventTotal) {
    int32_t eventTotal = sh->eventTotal;
    (void)taosThreadMutexUnlock(&sh->depLock);
    /* This is an internal invariant violation, so fail fast for the whole replay:
     * no rank can be published safely. Abort broadcasts depCond and wakes every lane
     * instead of allowing peers to wait forever on an impossible completion rank. */
    replayLaneAbort(sh, REPLAY_EXIT_INTERNAL);
    (void)fprintf(stderr, "replay: internal error, completion index %d outside event bounds %d\n",
                  selfIdx, eventTotal);
    return;
  }

  int32_t rank = sh->returnRankByEvent[selfIdx];
  for (int32_t edge = sh->dependentHead[selfIdx]; edge >= 0; edge = sh->dependentNext[edge]) {
    int32_t closeIdx = sh->dependentClose[edge];
    if (sh->closePending[closeIdx] > 0) sh->closePending[closeIdx]--;
  }
  if (rank >= 0) sh->completedReturns[rank] = true;
  while (sh->completedReturnUpto + 1 < sh->returnCount &&
         sh->completedReturns[sh->completedReturnUpto + 1]) {
    sh->completedReturnUpto++;
  }
  (void)taosThreadCondBroadcast(&sh->depCond);
  (void)taosThreadMutexUnlock(&sh->depLock);
}
