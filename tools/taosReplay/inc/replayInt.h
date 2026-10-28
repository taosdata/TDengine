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

#ifndef TDENGINE_REPLAY_INT_H_
#define TDENGINE_REPLAY_INT_H_

#include <stdbool.h>
#include <stdint.h>

#define REPLAY_BEGIN_HASH_MULTIPLIER UINT64_C(1099511628211)

#include "os.h"
#include "wlrFormat.h"

#ifdef __cplusplus
extern "C" {
#endif

#define REPLAY_MAX_INPUTS 32

/* Total segment files a run can load, shared across ALL --input paths (not per input).
 * The 8-segment-per-input factor is sizing headroom. Exceeding this hard global limit
 * aborts the complete load with REPLAY_EXIT_INPUT. */
#define REPLAY_MAX_SEGMENTS (REPLAY_MAX_INPUTS * 8)
#define REPLAY_MAX_MAPS   32
#define REPLAY_MAX_CREDS  32
/* Hard cap: reaching it aborts input loading with REPLAY_EXIT_INPUT rather than
 * silently truncating the event stream. */
#define REPLAY_MAX_EVENTS 250000

static inline bool replayPhaseTerminatesCall(uint16_t apiType, bool stmt2Async, uint16_t phase) {
  if (phase == WLR_PHASE_DONE) return true;
  if (phase != WLR_PHASE_RETURN) return false;
  return !(apiType == WLR_API_STMT2_EXEC && stmt2Async) &&
         apiType != WLR_API_QUERY_A && apiType != WLR_API_QUERY_A_WITH_REQID &&
         apiType != WLR_API_FETCH_ROWS_A && apiType != WLR_API_FETCH_RAW_BLOCK_A &&
         apiType != WLR_API_STMT2_BIND_PARAM_A && apiType != WLR_API_STMT2_BIND_PARAM_COLUMN_A;
}

/* Exit codes are interface, not implementation detail (FS 4.12). Code 1 is
 * deliberately reserved for uncategorized launcher/runtime failures; the tool
 * itself returns one of the classified values below. */
#define REPLAY_EXIT_OK       0
#define REPLAY_EXIT_INPUT    2
#define REPLAY_EXIT_CONN     3
#define REPLAY_EXIT_PARTIAL  4
#define REPLAY_EXIT_INTERNAL 5

typedef enum {
  REPLAY_MODE_NONE = 0,
  REPLAY_MODE_INSPECT,
  REPLAY_MODE_REPLAY,
} EReplayMode;

/* All pointer fields borrow argv storage and must not be freed. The parser maintains
 * 0 <= inputCount <= REPLAY_MAX_INPUTS, 0 <= endpointMapCount <= REPLAY_MAX_MAPS,
 * and 0 <= credCount <= REPLAY_MAX_CREDS. */
typedef struct {
  EReplayMode mode;
  const char *inputs[REPLAY_MAX_INPUTS];
  int32_t     inputCount;
  const char *targetHost;
  uint16_t    targetPort;
  const char *endpointMaps[REPLAY_MAX_MAPS];
  int32_t     endpointMapCount;
  const char *creds[REPLAY_MAX_CREDS];
  int32_t     credCount;
  const char *reportDir;
  bool        allowIncomplete;
  bool        verbose;
  int32_t     unfinishedTimeoutSec;
} SReplayOpts;

/*
 * One recorded event, flattened for replay.
 *
 * SReplaySession owns the event array. Every pointer field below is a read-only
 * borrow into one of that session's segment buffers; callers must not free or write
 * through it, and no event or borrowed pointer may be used after replayFreeSession.
 * replayFreeSession releases the event array and segment buffers together.
 *
 * All fields ending in Len are byte counts copied from the wire and exclude any
 * trailing NUL. Text therefore must be copied before passing it to a C-string API.
 */
typedef struct {
  /* Original accepted-event order before the cross-session time merge. It is the
   * final deterministic sort key when malformed input repeats every wire key. */
  int32_t loadOrdinal;
  /* Zero-based recording-session index assigned from the segment header identity.
   * Multiple explicit --input segments from one recording share this namespace. */
  int32_t  sessionIdx;
  /* Thread the call was made on during recording. One Worker Lane per distinct
   * value is what reproduces the original concurrency (DS 6.19). */
  uint64_t originTid;
  uint64_t eventSeq;
  uint64_t callId;
  uint64_t parentCallId;
  uint64_t connId;
  uint64_t stmtId;
  uint64_t resultId;
  uint16_t apiType;
  uint16_t phase;
  bool     stmt2Async;
  bool     stmtInitOptionsSet;
  uint8_t  stmtInitOptions;
  bool     haveMonoNs;
  /* Raw monotonic time while loading, then nanoseconds since the recording group's
   * single shared origin after all compatible sessions have been validated. */
  int64_t  monoNs;
  int64_t  wallNs;
  int64_t  reqid; /* query/STMT/SML variants that accept a request ID */
  int32_t  retCode;
  int64_t  affectedRows;
  int64_t  fetchRows;
  bool     affectedRowsSet;
  bool     fetchRowsSet;
  const char *sql;
  uint32_t    sqlLen;
  const char *tbname; /* STMT set-table-name calls */
  uint32_t    tbnameLen;
  const uint8_t *bindBytes; /* STMT/STMT2 bind calls */
  uint32_t       bindLen;
  int32_t        bindColIdx;
  bool           bindColIdxSet;
  int32_t        bindTableCount;
  bool           bindTableCountSet;
  const uint8_t *tbnameList;    /* STMT2 BINDV table names (count+lengths+bytes) */
  uint32_t       tbnameListLen;
  const uint8_t *stmt2Tags;
  uint32_t       stmt2TagsLen;
  const char    *endpoint;
  uint32_t       endpointLen;
  uint16_t       port;
  const char    *user;
  uint32_t       userLen;
  const char    *db;
  uint32_t       dbLen;
  const uint8_t *connOptList;
  uint32_t       connOptListLen;
  bool           connOptListOmitted;
  int32_t        connOptType;
  bool           connOptTypeSet;
  const char    *connOptName;
  uint32_t       connOptNameLen;
  const char    *connOptValue;
  uint32_t       connOptValueLen;
  bool           connOptValueSet;
  const uint8_t *smlPayload; /* SML calls only */
  uint32_t       smlPayloadLen;
  int32_t        smlProtocol;
  int32_t        smlPrecision;
  /* TTL and tbname key are only meaningful for the ttl/tbname-key SML variants.
   * TTL 0 is the API's canonical "unset/no TTL" value, so an omitted tag and an
   * explicitly recorded zero are intentionally equivalent. A NULL key is unset. */
  int32_t        smlTtl;
  const char    *smlTbnameKey;
  uint32_t       smlTbnameKeyLen;
  bool           smlTbnameKeySet;
} SReplayEvent;

typedef struct {
  uint8_t *buf; /* whole segment contents; events alias into this */
  int64_t  len;
} SReplayBuffer;

/* Approximately 5.3 KiB with the current constants, with a compile-time 16 KiB
 * stack-safety ceiling enforced below. The main thread may place it on a normal
 * process stack; callers running with a deliberately restricted thread stack should
 * allocate it on the heap instead. replayLoadInputs requires a zero-initialized session.
 * replayFreeSession releases its owned storage and restores that state before the
 * object may be loaded again. */
typedef struct {
  SReplayBuffer buffers[REPLAY_MAX_SEGMENTS];
  int32_t       bufferCount;

  SReplayEvent *events;
  int32_t       eventCount;
  int32_t       eventCap;

  /* Completeness, as observed while loading (DS 6.15.1). */
  bool structurallyComplete;
  bool sawSessionStop;
  bool sawFatal;
  bool tornTail;
  int32_t unfinishedCalls;
  bool unfinishedCountKnown;
  int32_t sessionCount;
  /* Indexed by recording-session identity rather than --input ordinal. */
  uint64_t lastEventSeqBySession[REPLAY_MAX_INPUTS];
  bool haveEventSeqBySession[REPLAY_MAX_INPUTS];
  bool truncatedBySession[REPLAY_MAX_INPUTS];
  uint8_t sessionIdBySession[REPLAY_MAX_INPUTS][WLR_SESSION_ID_LEN];
  char clockDomainIdBySession[REPLAY_MAX_INPUTS][WLR_CLOCK_DOMAIN_ID_LEN];
  uint64_t pidBySession[REPLAY_MAX_INPUTS];
  int64_t processStartWallNsBySession[REPLAY_MAX_INPUTS];
  int64_t processStartMonoNsBySession[REPLAY_MAX_INPUTS];
  uint32_t lastSegmentIndexBySession[REPLAY_MAX_INPUTS];
  bool haveSegmentIndexBySession[REPLAY_MAX_INPUTS];
} SReplaySession;
#ifdef __cplusplus
static_assert(sizeof(SReplaySession) <= 16 * 1024, "SReplaySession exceeds stack-safety budget");
#else
_Static_assert(sizeof(SReplaySession) <= 16 * 1024, "SReplaySession exceeds stack-safety budget");
#endif

/* replayInput.c */
/* Returns one of the REPLAY_EXIT_* codes defined above. */
int32_t replayLoadInputs(const SReplayOpts *opts, SReplaySession *session);
void    replayFreeSession(SReplaySession *session);
void    replayPrintInspect(const SReplayOpts *opts, const SReplaySession *session);

/* replayExec.c */
int32_t replayExecute(const SReplayOpts *opts, SReplaySession *session);

#ifdef __cplusplus
}
#endif

#endif /* TDENGINE_REPLAY_INT_H_ */
