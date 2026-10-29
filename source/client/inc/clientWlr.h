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
 * Workload Recorder (WLR) -- client-private interface (DS 6.2, 7.3).
 *
 * NOT part of taos.h. Nothing here is exported: the recorder adds no public ABI
 * (FS 7.5). Public API wrappers in clientMain.c reach it only through the thin
 * typed macros at the bottom of this header.
 *
 * Design rules this interface encodes:
 *
 *  1. Recording is decided once, at process startup, from client config. There
 *     is no runtime enable/disable, so no object-state bootstrap is needed for
 *     connections/statements/results that already exist (FS 4.2).
 *  2. BEGIN is written and acknowledged BEFORE the real API is invoked, so a
 *     call that never returns still leaves a record (DS 6.14).
 *  3. Fail-open: any recorder failure stops recording and releases blocked
 *     callers; the database API keeps working (FS 4.8). No function here
 *     returns an error that a caller is expected to propagate to the user.
 *  4. Only the outermost user call is recorded. A public API that internally
 *     calls another public entry point must not produce a second event
 *     (FS 4.1) -- see wlrApiBegin's depth rule.
 *  5. No platform-specific code and no direct libc syscalls (DS 4.3.1).
 */
#ifndef TDENGINE_CLIENT_WLR_H_
#define TDENGINE_CLIENT_WLR_H_

#if defined(__GNUC__) && !defined(_WIN32)
#define WLR_INTERNAL_HIDDEN __attribute__((visibility("hidden")))
#else
#define WLR_INTERNAL_HIDDEN
#endif

#include "os.h"
#include "tdef.h"
#include "wlrFormat.h"

#ifdef __cplusplus
extern "C" {
#endif

/* --------------------------------------------------------------- lifecycle */

/*
 * Manager state (DS 6.3).
 *
 * Stored as int32_t volatile and accessed through atomic_load_32 /
 * atomic_val_compare_exchange_32. Not C11 _Atomic: community/core must build
 * under GCC 7, and the Windows build does not guarantee _Atomic (DS 4.3).
 */
typedef enum {
  WLR_STATE_OFF = 0,          /* not recording; hot path does one atomic load */
  WLR_STATE_STARTING = 1,     /* init in progress, not yet accepting events */
  WLR_STATE_RUNNING = 2,      /* first segment header written */
  WLR_STATE_STOPPING = 3,     /* draining; no new tokens are issued */
  WLR_STATE_FAILED_OPEN = 4,  /* permanent failure; hooks bypass, business ok */
} EWlrState;

typedef struct {
  int32_t mode;                          /* WLR_CFG_MODE_OFF / WLR_CFG_MODE_ON; other values reserved */
  int32_t bufferSizeMb;                  /* WLR_CFG_BUFFER_MB_MIN..MAX */
  char    recordDir[PATH_MAX];  /* already expanded by tglobal */
} SWorkloadRecorderCfg;

/*
 * Start recording. Called once from taos_init_imp() after config and logging
 * are up.
 *
 * Returns an EWlrError. The caller MUST NOT abort client startup on failure:
 * a recording problem may not prevent the driver from working (FS 4.8). Log it
 * and continue.
 *
 * mode 0 and reserved modes 2..255 allocate nothing at all -- no queue, no
 * pool, no writer thread, no directory (DS 6.1).
 */
WLR_INTERNAL_HIDDEN int32_t wlrRecorderInit(const SWorkloadRecorderCfg *cfg);

/*
 * Convenience wrapper that reads the three client config globals and calls
 * wlrRecorderInit. This is what taos_init_imp() uses, so the driver init path
 * does not need to know the config variable names.
 */
WLR_INTERNAL_HIDDEN int32_t wlrRecorderInitFromCfg(void);

/*
 * Stop recording: drain the queue, write SESSION_STOP with the still-open call
 * list, flush, close files, release the pool. Idempotent, and safe when the
 * recorder never started.
 *
 * Must not wait on in-flight user API calls. A hung query holds a Call ID that
 * never completes; stopping records it as unfinished and moves on (DS 6.4).
 *
 * Residual limit: the drain timeout cannot interrupt a writer thread already blocked
 * inside a filesystem write. In that case close waits for the kernel call to return;
 * detaching the writer would let it access the queue and segment after they are freed.
 */
WLR_INTERNAL_HIDDEN void wlrRecorderClose(void);
WLR_INTERNAL_HIDDEN bool wlrRecorderReleaseLog(void);

/* Current state, for tests and diagnostics. */
WLR_INTERNAL_HIDDEN int32_t wlrRecorderState(void);

/* ------------------------------------------------------------ call capture */

/*
 * Per-call token. Lives on the caller's stack for the duration of one public
 * API call, so it must stay small and must not own heap memory.
 *
 * recorded=false means "this call is not being recorded" -- either the recorder
 * is off, or this is a nested public call (see the depth rule). Every End
 * function tolerates such a token and does nothing.
 */
typedef struct {
  uint64_t callId;
  uint64_t eventSeq;
  int64_t  beginMonoNs;
  uint16_t apiType;
  bool     recorded;
  bool     depthOwner; /* true when this call incremented the TLS depth */
} SWorkloadCallToken;

/*
 * Arguments for one recorded call.
 *
 * Two distinct guarantees apply to these fields, and it is worth not conflating
 * them:
 *
 *  1. No CREDENTIAL reaches the file. Passwords, tokens and TOTP codes are never
 *     passed to the recorder at all (FS 6.2) -- not masked, not truncated, simply
 *     absent from every struct it sees. connOptValue cannot carry one either; see
 *     the audit of TSDB_OPTION_CONNECTION below.
 *
 *  2. Application DATA is recorded verbatim, deliberately. sql, smlLines, smlRaw,
 *     the bind payloads and tbname are stored unmasked because a masked statement
 *     cannot be replayed (FS 6.3), which is the entire purpose of the format. So a
 *     segment does contain whatever the application itself put in its statements
 *     and rows -- including a literal secret a user chose to embed in SQL. That is
 *     not a recorder leak, but it does make the file as sensitive as the database
 *     content it describes.
 *
 * Which is why the file permissions are part of the contract rather than incidental:
 * wlrPrepareDir forces the record dir to WLR_DIR_PERM (0700) and refuses to record
 * into a directory the operator made unwritable, and every segment and manifest is
 * opened TD_FILE_PRIVATE (0600). Recording is also startup-only, so the destination
 * cannot be redirected by a running application. Enabling the recorder is therefore
 * a decision to persist plaintext workload content, owner-readable only.
 */
typedef struct {
  uint16_t    apiType; /* EWlrApiType */
  uint64_t    logicalConnId;
  uint64_t    logicalStmtId;
  uint64_t    logicalResultId;
  int64_t     reqid;
  const char *sql; /* borrowed for the duration of the call only */
  int32_t     sqlLen;

  /* connect */
  const char *endpoint;
  const char *user;
  const char *db;
  uint16_t    port;

  /*
   * Connection option (taos_options_connection, and the option bag that
   * taos_connect_with unpacks). The option changes how later statements are
   * interpreted, so replay must reapply the same one with the same value --
   * recording only that "some option was set" is not replayable.
   *
   * The value is written to the segment verbatim, so it must not be able to carry a
   * credential. TSDB_OPTION_CONNECTION (taos.h) is a closed enum, and none of its
   * members is a secret: CLEAR, CHARSET, TIMEZONE, USER_IP, USER_APP and
   * CONNECTOR_INFO. Passwords, tokens and TOTP codes travel as connect() arguments,
   * which the recorder never receives (FS 6.2) -- there is no option-shaped path for
   * them. A new member of that enum that DID carry a secret would need redaction
   * here before it could be recorded; that is the check to make when the enum grows.
   *
   * connOptSet distinguishes "option 0 with a NULL value" (clearing charset, a
   * real call) from "no option on this event", which a zero type cannot.
   */
  int32_t     connOptType;
  const char *connOptName;  /* stable spelling for humans reading the file */
  const char *connOptValue; /* NULL means the caller cleared the option */
  bool        connOptSet;

  /* Whole option set for taos_connect_with, already encoded by the caller
   * (count + entry table + bytes) via wlrEncodeConnOptList. */
  const void *connOptList;
  int32_t     connOptListLen;
  bool        connOptListOmitted;

  /* schemaless */
  int32_t     smlProtocol;
  int32_t     smlPrecision;
  int32_t     smlTtl;
  int32_t     smlCount;
  const char *smlTbnameKey;
  const char *const *smlLines;
  const char *smlRaw;
  int32_t     smlRawLen;

  /* stmt bind: column-wise description, never a raw bind struct (DS 6.9) */
  const void *bindDesc;
  int32_t     bindDescLen;
  /*
   * Target column for a single-column bind. bindColIdxSet distinguishes "bound
   * column 0" from "this event has no column index", which a zero cannot: the
   * row-mode APIs use -1 for all-columns, so 0 is a real, distinct instruction.
   */
  int32_t     bindColIdx;
  bool        bindColIdxSet;
  /* Column-mode STMT2 table groups. BIND_COUNT carries this only when set; zero is
   * meaningful for single-table/query binds, so presence is tracked separately. */
  int32_t     bindTableCount;
  bool        bindTableCountSet;
  bool        stmt2Async;
  uint8_t     stmtInitOptions;
  bool        stmtInitOptionsSet;
  const char *tbname; /* STMT set_tbname target, borrowed for the call */
  /* STMT2 BINDV table names, already encoded by the caller (count+lengths+bytes). */
  const void *tbnameList;
  int32_t     tbnameListLen;
  const void *stmt2Tags;
  int32_t     stmt2TagsLen;
} SWorkloadBeginArgs;

typedef struct {
  int32_t  code;
  uint64_t logicalConnId;   /* assigned by connect */
  uint64_t logicalStmtId;   /* assigned by stmt init */
  uint64_t logicalResultId; /* assigned by query/execute/use_result */
  int64_t  affectedRows;
  int64_t  fetchRows;
  int64_t  qid;
} SWorkloadReturnArgs;

typedef struct {
  int32_t  code;
  uint64_t logicalResultId;
  uint64_t logicalStmtId;
  int64_t  affectedRows;
  int64_t  fetchRows;
} SWorkloadDoneArgs;

/*
 * Record the start of a public API call and wait until the BEGIN frame has been
 * written (DS 6.14). Returns 0 when recording succeeded or when a nested call was
 * intentionally skipped. Callers that need to distinguish those cases inspect
 * token->recorded; the public-API macros deliberately ignore the return code.
 *
 * Depth rule (FS 4.1, DS 6.6): recording happens only when the calling thread's
 * public-API depth goes 0 -> 1. A public API that internally calls another
 * public entry point sees depth > 1 and is not recorded, so replay does not
 * execute the work twice.
 *
 * IMPORTANT: depth is per thread. An internal call issued on a driver
 * completion thread observes depth 0 and WOULD be recorded. Depth cannot fix
 * that; such paths must call an internal *Impl() function instead of the public
 * entry point. The known instance is postSubQueryFetchCb -> taos_fetch_row in
 * clientImpl.c (TS RE014).
 *
 * When recording is unavailable -- recorder off, queue gone, writer dead, or a
 * BEGIN acknowledgement fails -- this returns non-zero with token->recorded = false.
 * A nested call is intentionally skipped and returns success with
 * token->recorded = false. In every case the caller proceeds into the real API
 * regardless: BEGIN-before-call is the healthy-recorder guarantee, while failure
 * degrades to fail-open rather than blocking the business call.
 */
WLR_INTERNAL_HIDDEN int32_t wlrApiBegin(const SWorkloadBeginArgs *args, SWorkloadCallToken *token);
WLR_INTERNAL_HIDDEN void    wlrApiSkipToken(SWorkloadCallToken *token);

/*
 * Record the return of a public API call. It does not wait for a write acknowledgement;
 * only BEGIN does (DS 9.2). It may still wait briefly for queue space under backpressure.
 *
 * Must not dereference the caller's original arguments: by now they may have
 * been freed or reused. Only the token and the return values are valid
 * (DS 6.6).
 */
WLR_INTERNAL_HIDDEN void wlrApiReturn(SWorkloadCallToken *token, const SWorkloadReturnArgs *args);

/* Record async completion, just before the user callback runs (DS 6.8). */
WLR_INTERNAL_HIDDEN void wlrAsyncDone(SWorkloadCallToken *token, const SWorkloadDoneArgs *args);

/* Placeholder for future stateful result-ID cleanup. Result IDs are currently
 * resolved only from the event stream, so this call intentionally has no effect. */
WLR_INTERNAL_HIDDEN void wlrResultFree(uint64_t logicalResultId);

/* Mint a successful connection's logical ID when needed and emit its RETURN. */
WLR_INTERNAL_HIDDEN void wlrConnectComplete(SWorkloadCallToken *token, uint64_t *logicalConnId);

/* -------------------------------------------------------------- ID minting */

WLR_INTERNAL_HIDDEN uint64_t wlrNextConnId(void);
WLR_INTERNAL_HIDDEN uint64_t wlrNextStmtId(void);
WLR_INTERNAL_HIDDEN uint64_t wlrNextResultId(void);

/* ------------------------------------------------- async callback plumbing */

/*
 * Set just before invoking a user callback so that public API calls made inside
 * it record a parentCallId, letting replay honour "not before this completed"
 * (DS 6.8).
 *
 * The caller must restore the previous value afterwards, rather than always clearing
 * to 0: callbacks may nest on the same driver thread. A stale or prematurely cleared
 * value would misattribute a later call (TS API027b).
 */
WLR_INTERNAL_HIDDEN void     wlrSetCallbackParent(uint64_t callId);
WLR_INTERNAL_HIDDEN uint64_t wlrGetCallbackParent(void);
WLR_INTERNAL_HIDDEN int32_t  wlrCallbackBegin(void);
WLR_INTERNAL_HIDDEN void     wlrCallbackEnd(int32_t previousDepth);
WLR_INTERNAL_HIDDEN int32_t  wlrInternalBegin(void);
WLR_INTERNAL_HIDDEN void     wlrInternalEnd(int32_t previousDepth);

typedef struct {
  uint64_t previousParent;
  int32_t  previousDepth;
} SWlrCallbackScope;

WLR_INTERNAL_HIDDEN void wlrCallbackScopeBegin(uint64_t parentCallId, SWlrCallbackScope *scope);
WLR_INTERNAL_HIDDEN void wlrCallbackScopeEnd(const SWlrCallbackScope *scope);

/*
 * Hand an async API's call down to the request the driver creates for it, so the
 * eventual completion callback can name it as the parent (DS 6.8) and emit
 * REQUEST_DONE: the DONE event carries apiType and a duration measured from
 * beginMonoNs, neither of which can be recovered from the Call ID alone.
 *
 * Thread-local and stack-ordered: the public wrapper pushes immediately before
 * calling into the driver, request creation takes the top entry on the same thread,
 * and the wrapper pairs that operation with wlrSetPendingAsyncCall(NULL) after the
 * driver returns. The stack preserves an outer token if a synchronous callback starts
 * another async public API on the same thread. An unrecorded token pushes an empty
 * entry so the nesting remains balanced.
 *
 * The three fields are deliberately set and taken as a unit, with no single-field
 * accessor. Clearing the ID alone would leave beginMonoNs and apiType behind for the
 * next taker to read, attributing an earlier call's metadata to a new request --
 * so the API does not offer a way to do it.
 *
 * Out params of wlrTakePendingAsyncCall may be NULL. NULL passed to the setter is
 * the paired wrapper cleanup operation; it must be issued once for every non-NULL set.
 */
WLR_INTERNAL_HIDDEN void wlrSetPendingAsyncCall(const SWorkloadCallToken *token);
WLR_INTERNAL_HIDDEN void wlrTakePendingAsyncCall(uint64_t *callId, int64_t *beginMonoNs, uint16_t *apiType);
#undef WLR_INTERNAL_HIDDEN

/* ------------------------------------------------------------- test hooks */

/*
 * Fault injection and introspection for unit tests (TS CFG02x, ORD00x, FO00x).
 * Backpressure and fail-open cannot be covered without these: there is no
 * portable way to fill a disk or stall a thread from a test otherwise.
 *
 * Compiled only into the test-linked static client library. Production builds do
 * not compile these declarations; GNU builds additionally mark them hidden.
 */
#ifdef WLR_TEST_HOOKS
#if defined(__GNUC__) && !defined(_WIN32)
#define WLR_TEST_HIDDEN __attribute__((visibility("hidden")))
#else
#define WLR_TEST_HIDDEN
#endif

WLR_TEST_HIDDEN uint64_t wlrTokenEventSeq(const SWorkloadCallToken *token);
WLR_TEST_HIDDEN uint64_t wlrTestLastEventSeq(void);
WLR_TEST_HIDDEN int32_t  wlrTestCurrentDepth(void);
WLR_TEST_HIDDEN uint64_t wlrTestDroppedEventCount(void);
WLR_TEST_HIDDEN int64_t  wlrTestEventsWritten(void);
WLR_TEST_HIDDEN int64_t  wlrTestBackpressureNs(void);
WLR_TEST_HIDDEN int32_t  wlrTestAdmissionWaiters(void);
WLR_TEST_HIDDEN int32_t  wlrTestActiveCallCount(void);
WLR_TEST_HIDDEN void     wlrTestResetDepth(void);
WLR_TEST_HIDDEN int32_t  wlrTestInjectWriterFailure(void);
WLR_TEST_HIDDEN int32_t  wlrTestInjectFsyncFailure(void);
WLR_TEST_HIDDEN int32_t  wlrTestClearWriterFailure(void);
WLR_TEST_HIDDEN int32_t  wlrTestStallWriter(void);
WLR_TEST_HIDDEN int32_t  wlrTestSetStopping(void);
WLR_TEST_HIDDEN int32_t  wlrTestResumeWriter(void);
WLR_TEST_HIDDEN int32_t  wlrTestEnterLifecycle(void);
WLR_TEST_HIDDEN void     wlrTestLeaveLifecycle(void);
WLR_TEST_HIDDEN void     wlrTestCloseLifecycleAdmission(void);
#undef WLR_TEST_HIDDEN
#endif

/* ------------------------------------------------------------- thin macros */

/*
 * Typed per-group macros (DS 6.2). Deliberately one macro per API shape rather
 * than a single variadic macro: the required-tag schema differs per group, so a
 * catch-all macro would defer schema mistakes to runtime.
 *
 * These only build an argument description and call a private function. No
 * encoding, no locking, no file I/O happens in the wrapper itself.
 *
 * When recording is off, the cost is one atomic load inside wlrApiBegin
 * (DS 9.1).
 * wlrApiBegin always clears a non-NULL token before it can fail; BEGIN macros may
 * therefore discard its status while their paired END observes recorded=false.
 */

/*
 * No query BEGIN macro. Query BEGIN requires CONN_ID (kQueryBeginTags), and
 * resolving it means acquiring the TSC object behind the TAOS* handle and releasing
 * it again -- acquireTscObj/releaseTscObj around a wlrConnId read. That is a
 * multi-statement sequence with a lifetime pairing to honour, not something a macro
 * can express, and a macro that took the handle and quietly ignored it would produce
 * BEGIN events that fail their own schema. taos_query and friends build
 * SWorkloadBeginArgs directly and do the acquire/release inline; see clientMain.c.
 */

/* The credential-bearing variants (totp/auth/token) share this shape but carry a
 * different API type; the secret itself is never an argument here (FS 6.2).
 * Current public callers pass uint16_t ports, so preserving that value requires no
 * runtime range normalization. BEGIN and END both guard the call on a non-NULL token,
 * which keeps the per-thread depth balanced. */
#define WLR_API_BEGIN_CONNECT_TYPED(_tok, _api, _ep, _user, _db, _port) \
  do {                                                                 \
    SWorkloadCallToken *_t0 = (_tok);                                  \
    SWorkloadBeginArgs _a = {0};                                       \
    _a.apiType = (uint16_t)(_api);                                     \
    _a.endpoint = (_ep);                                               \
    _a.user = (_user);                                                 \
    _a.db = (_db);                                                     \
    _a.port = (uint16_t)(_port);                                       \
    if (_t0 != NULL) (void)wlrApiBegin(&_a, _t0);                      \
  } while (0)

#define WLR_API_BEGIN_CONNECT(_tok, _ep, _user, _db, _port) \
  WLR_API_BEGIN_CONNECT_TYPED(_tok, WLR_API_CONNECT, _ep, _user, _db, _port)

/*
 * Fetch group. The result ID lives on the SRequestObj, so the caller passes it in
 * rather than the handle: reading it here would require this header to know the
 * client's internal struct, which it deliberately does not.
 *
 * A zero result ID means the result was never handed to the user (an internal
 * request), so nothing is recorded.
 */
#define WLR_API_BEGIN_FETCH(_tok, _api, _resId)             \
  do {                                                      \
    SWorkloadCallToken *_t0 = (_tok);                        \
    const uint64_t     _rid0 = (uint64_t)(_resId);           \
    SWorkloadBeginArgs _a = {0};                             \
    _a.apiType = (uint16_t)(_api);                           \
    _a.logicalResultId = _rid0;                              \
    if (_rid0 != 0) {                                        \
      (void)wlrApiBegin(&_a, _t0);                           \
    } else {                                                 \
      if (_t0 != NULL) {                                     \
        wlrApiSkipToken(_t0);                                 \
      }                                                       \
    }                                                        \
  } while (0)

#define WLR_API_END_FETCH(_tok, _code, _resId, _rows) \
  do {                                                \
    /* BEGIN's recorded flag already captures whether resId was nonzero. */ \
    SWorkloadCallToken *_t0 = (_tok);                 \
    SWorkloadReturnArgs _r = {0};                     \
    _r.code = (int32_t)(_code);                       \
    _r.logicalResultId = (uint64_t)(_resId);          \
    _r.fetchRows = (int64_t)(_rows);                  \
    if (_t0 != NULL) wlrApiReturn(_t0, &_r);          \
  } while (0)

/*
 * STMT group. All 24 STMT1/STMT2 entry points share one required-tag shape
 * (common + stmt ID), so one begin macro covers them; the API type distinguishes
 * them in the file.
 *
 * SQL is passed for prepare, bind descriptions for bind calls, NULL otherwise.
 * A zero stmt ID means the handle predates recording, so nothing is recorded.
 *
 * The ID is read into a local so it is evaluated exactly once, and the skip path
 * clears the token through the same helper used by the capture implementation. The paired END macro accepts a
 * NULL token and otherwise calls wlrApiReturn, which reads depthOwner: a stale or
 * uninitialised true there would decrement the thread's API depth for a call that was
 * never recorded. Callers do zero their tokens today, but the macro must not depend
 * on it.
 */
#define WLR_API_BEGIN_STMT(_tok, _api, _stmtId, _sql, _sqlLen) \
  do {                                                         \
    SWorkloadCallToken *_t0 = (_tok);                          \
    const uint64_t     _sid0 = (uint64_t)(_stmtId);            \
    const char        *_sql0 = (_sql);                          \
    const int64_t      _sqlLen0 = (int64_t)(_sqlLen);           \
    const size_t       _effectiveSqlLen0 = (_sql0 != NULL && _sqlLen0 == 0) ? strlen(_sql0) : \
                                                    (_sqlLen0 > 0 ? (size_t)_sqlLen0 : 0); \
    const bool         _sqlLenValid = _sql0 == NULL ||                         \
                                              (_sqlLen0 >= 0 && _effectiveSqlLen0 <= INT32_MAX); \
    SWorkloadBeginArgs _a = {0};                               \
    _a.apiType = (uint16_t)(_api);                             \
    _a.logicalStmtId = _sid0;                                  \
    _a.sql = _sqlLenValid ? _sql0 : NULL;                       \
    _a.sqlLen = _sqlLenValid && _sql0 != NULL ? (int32_t)_effectiveSqlLen0 : 0; \
    if (_sid0 != 0) {                                          \
      (void)wlrApiBegin(&_a, _t0);                             \
    } else {                                                   \
      if (_t0 != NULL) {                                      \
        wlrApiSkipToken(_t0);                                 \
      }                                                        \
    }                                                          \
  } while (0)

#define WLR_API_END_STMT(_tok, _code, _stmtId)  \
  do {                                          \
    SWorkloadCallToken *_t0 = (_tok);           \
    SWorkloadReturnArgs _r = {0};                \
    _r.code = (int32_t)(_code);                  \
    _r.logicalStmtId = (uint64_t)(_stmtId);      \
    if (_t0 != NULL) wlrApiReturn(_t0, &_r);     \
  } while (0)

/*
 * No schemaless BEGIN macro either. Schemaless BEGIN needs protocol, precision, ttl,
 * reqid and either a line vector or a raw buffer -- and the line-vector form has to
 * encode the lines before it can record them. clientSml.c's smlInsertLines and its
 * raw counterpart build the args directly; each obtains the connection ID from the
 * taos handle using the SML path's existing handle lifetime rules. Protocol and
 * precision are required by the schema because replay must call the same variant
 * with the same interpretation of the payload (DS 6.10).
 */

#define WLR_API_END_CODE(_tok, _code)       \
  do {                                      \
    SWorkloadCallToken *_t0 = (_tok);       \
    SWorkloadReturnArgs _r = {0};           \
    _r.code = (int32_t)(_code);             \
    if (_t0 != NULL) wlrApiReturn(_t0, &_r); \
  } while (0)

#define WLR_API_END_RESULT(_tok, _code, _resId) \
  do {                                          \
    SWorkloadCallToken *_t0 = (_tok);           \
    SWorkloadReturnArgs _r = {0};               \
    _r.code = (int32_t)(_code);                 \
    _r.logicalResultId = (uint64_t)(_resId);    \
    if (_t0 != NULL) wlrApiReturn(_t0, &_r);    \
  } while (0)

#ifdef __cplusplus
}
#endif

#endif /* TDENGINE_CLIENT_WLR_H_ */
