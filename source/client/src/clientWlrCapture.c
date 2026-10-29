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
 * Workload Recorder -- capture hooks (DS 6.6, 6.7, 6.8).
 *
 * This file turns a public API call into TLV frames. It contains no file I/O and
 * no encoding of the frame envelope: it fills a reserved payload buffer and hands
 * the slot to the writer.
 *
 * Two rules dominate the code below.
 *
 *   Outermost-only: recording happens when the calling thread's public API depth
 *   goes 0 -> 1. A public API that internally calls another public entry point
 *   sees depth > 1 and records nothing, so replay does not execute the same work
 *   twice (FS 4.1).
 *
 *   Never touch caller memory after the call: wlrApiReturn runs after the real
 *   API has returned, by which point the caller's sql/bind pointers may be freed.
 *   Only the token and the return values are read there (DS 6.6).
 */

#include "clientWlrInt.h"
#include "taos.h"

static bool wlrApiIsSml(uint16_t apiType) {
  switch (apiType) {
    case WLR_API_SML_INSERT:
    case WLR_API_SML_INSERT_WITH_REQID:
    case WLR_API_SML_INSERT_RAW:
    case WLR_API_SML_INSERT_RAW_WITH_REQID:
    case WLR_API_SML_INSERT_TTL:
    case WLR_API_SML_INSERT_TTL_WITH_REQID:
    case WLR_API_SML_INSERT_RAW_TTL:
    case WLR_API_SML_INSERT_RAW_TTL_WITH_REQID:
    case WLR_API_SML_INSERT_RAW_TTL_WITH_REQID_TBNAME_KEY:
    case WLR_API_SML_INSERT_TTL_WITH_REQID_TBNAME_KEY:
      return true;
    default:
      return false;
  }
}

/* Room for the common tag set plus the largest fixed group (connect: endpoint,
 * user, db, port). Variable-length values are bounded separately. */
#define WLR_TLV_FIXED_MAX 512
/* A TLV occupies at least its wire header. Keeping the schema-check capacity
 * derived from the fixed metadata budget avoids a second magic limit. */
#define WLR_SCHEMA_CHECK_STACK_TLVS (WLR_TLV_FIXED_MAX / WLR_TLV_HEADER_SIZE)

/*
 * TLV append helpers. Each returns the new offset, or -1 so a caller can chain with
 * `if (off > 0)` and roll the whole payload back on the first failure.
 *
 * Both re-check off against cap even though every call site guards with off > 0. The
 * guard proves the previous append succeeded; it does NOT prove there is room left.
 * Room is guaranteed by wlrBeginPayloadBound (or WLR_TLV_FIXED_MAX for completions)
 * being a strict upper bound -- an invariant spread across two functions with nothing
 * enforcing the pairing, so adding a variable-length tag without updating the bound is
 * an easy mistake. Without this check that mistake is memory corruption rather than a
 * dropped tag: off could advance to or past cap while still positive, (size_t)(cap-off)
 * would wrap to a huge value, the bounds check inside wlrEncodeTlv would pass, and the
 * write would land outside buf. Failing closed turns a bound regression into a lost
 * tag, which wlrCheckSchema then reports.
 */
static int32_t wlrPutTlvU64(uint8_t *buf, int32_t cap, int32_t off, uint16_t tag, uint64_t v) {
  int32_t n;
  if (off < 0 || off >= cap) return -1;
  n = wlrEncodeTlvU64(buf + off, (size_t)(cap - off), tag, true, v);
  return n <= 0 ? -1 : off + n;
}

static int32_t wlrPutTlvU64Optional(uint8_t *buf, int32_t cap, int32_t off, uint16_t tag,
                                    uint64_t v) {
  int32_t n;
  if (off < 0 || off >= cap) return -1;
  n = wlrEncodeTlvU64(buf + off, (size_t)(cap - off), tag, false, v);
  return n <= 0 ? -1 : off + n;
}

static int32_t wlrPutTlvRaw(uint8_t *buf, int32_t cap, int32_t off, uint16_t tag, bool required,
                            const void *p, uint32_t len) {
  int32_t n;
  if (off < 0 || off >= cap) return -1;
  n = wlrEncodeTlv(buf + off, (size_t)(cap - off), tag, required, 0, p, len);
  return n <= 0 ? -1 : off + n;
}

/* These helpers encode optional values: NULL and empty mean the tag is absent.
 * Required values, where present-but-empty differs from missing, must use
 * wlrPutRequiredBytesOrEmpty instead. */
static int32_t wlrPutTlvBytes(uint8_t *buf, int32_t cap, int32_t off, uint16_t tag, const void *p,
                              int32_t len) {
  if (p == NULL || len <= 0) return off;
  if (off < 0 || off >= cap) return -1;
  return wlrPutTlvRaw(buf, cap, off, tag, true, p, (uint32_t)len);
}

static int32_t wlrPutTlvStr(uint8_t *buf, int32_t cap, int32_t off, uint16_t tag, const char *s) {
  if (s == NULL || s[0] == 0) return off;
  return wlrPutTlvRaw(buf, cap, off, tag, false, s, (uint32_t)strlen(s));
}

static int32_t wlrPutRequiredBytesOrEmpty(uint8_t *buf, int32_t cap, int32_t off, uint16_t tag,
                                          const void *value, uint32_t length, bool *omitted) {
  int32_t next = (value != NULL && length > 0)
                     ? wlrPutTlvRaw(buf, cap, off, tag, true, value, length)
                     : -1;
  if (next > 0) return next;
  if (omitted != NULL) *omitted = true;
  return wlrPutTlvRaw(buf, cap, off, tag, true, NULL, 0);
}

static bool wlrConnOptValueSafe(int32_t type) {
  switch (type) {
    case TSDB_OPTION_CONNECTION_CLEAR:
    case TSDB_OPTION_CONNECTION_CHARSET:
    case TSDB_OPTION_CONNECTION_TIMEZONE:
    case TSDB_OPTION_CONNECTION_USER_IP:
    case TSDB_OPTION_CONNECTION_USER_APP:
    case TSDB_OPTION_CONNECTION_CONNECTOR_INFO:
      return true;
    default:
      return false;
  }
}

/*
 * Upper bound on the payload for one BEGIN event, so admission can reserve
 * before encoding. Overestimating only wastes pool space briefly; underestimating
 * would truncate an event, so every variable field is counted with its 8-byte TLV
 * header.
 *
 * Accumulated in uint64_t and saturated, for the same reason wlrBindEncodedSize and
 * wlrSmlLinesEncodedSize are: a uint32_t accumulator summing a dozen caller-supplied
 * lengths can wrap to a small value, and a small value PASSES the
 * bound > WLR_MAX_FRAME_PAYLOAD gate -- leaving metaOnly false and reserving a slot
 * too small for the payload that is then encoded into it. That is precisely the
 * underestimate this function exists to prevent. wlrSmlLinesEncodedSize alone can
 * return UINT32_MAX, which wraps on the +8. Saturating instead means the gate fires
 * and the event is recorded metadata-only, which is the intended handling for a
 * payload that cannot fit.
 */
static uint32_t wlrBeginPayloadBound(const SWorkloadBeginArgs *args) {
  /* Worst-case reservation: metaOnly omits optional fields, but normal encoding
   * may include all of them. */
  uint64_t n = WLR_TLV_FIXED_MAX;
  if (args->sql != NULL && args->sqlLen > 0) n += (uint64_t)args->sqlLen + WLR_TLV_HEADER_SIZE;
  if (args->endpoint != NULL) n += (uint64_t)strlen(args->endpoint) + WLR_TLV_HEADER_SIZE;
  if (args->user != NULL) n += (uint64_t)strlen(args->user) + WLR_TLV_HEADER_SIZE;
  if (args->db != NULL) n += (uint64_t)strlen(args->db) + WLR_TLV_HEADER_SIZE;
  if (args->connOptName != NULL) n += (uint64_t)strlen(args->connOptName) + WLR_TLV_HEADER_SIZE;
  if (args->connOptValue != NULL && wlrConnOptValueSafe(args->connOptType))
    n += (uint64_t)strlen(args->connOptValue) + WLR_TLV_HEADER_SIZE;
  if (args->connOptList != NULL && args->connOptListLen > 0)
    n += (uint64_t)args->connOptListLen + WLR_TLV_HEADER_SIZE;
  if (args->bindDesc != NULL && args->bindDescLen > 0)
    n += (uint64_t)args->bindDescLen + WLR_TLV_HEADER_SIZE;
  if (args->tbname != NULL) n += (uint64_t)strlen(args->tbname) + WLR_TLV_HEADER_SIZE;
  if (args->tbnameList != NULL && args->tbnameListLen > 0)
    n += (uint64_t)args->tbnameListLen + WLR_TLV_HEADER_SIZE;
  if (args->stmt2Tags != NULL && args->stmt2TagsLen > 0)
    n += (uint64_t)args->stmt2TagsLen + WLR_TLV_HEADER_SIZE;
  if (args->smlRaw != NULL && args->smlRawLen > 0)
    n += (uint64_t)args->smlRawLen + WLR_TLV_HEADER_SIZE;
  if (args->smlTbnameKey != NULL)
    n += (uint64_t)strlen(args->smlTbnameKey) + WLR_TLV_HEADER_SIZE;
  if (args->smlLines != NULL && args->smlCount > 0) {
    n += (uint64_t)wlrSmlLinesEncodedSize(args->smlLines, args->smlCount) + WLR_TLV_HEADER_SIZE;
  }
  return (n > UINT32_MAX) ? UINT32_MAX : (uint32_t)n;
}

/*
 * Verify an encoded payload satisfies the shared required-tag table before it is
 * published. DS 6.15.2 specifies validation on both sides of the format: the
 * recorder before writing, the parser after reading, against this one table.
 *
 * Runs unconditionally rather than under a debug flag. The cost is a TLV header
 * walk over a buffer already in cache with no copying -- negligible next to the
 * payload memcpy that just happened. The benefit is that a schema mistake names
 * the offending API at the moment it is instrumented; without it the error only
 * appears when something later decodes the file, which is exactly how the missing
 * CONN_ID on query BEGIN went unnoticed through 42 passing tests.
 *
 * The return value lets BEGIN reject a structurally invalid payload before it is
 * published. Completion events retain the existing fail-open behavior: their
 * caller cannot retry them, so the diagnostic is still more useful than silence.
 */
static bool wlrCheckSchema(uint16_t apiType, uint16_t phase, const uint8_t *payload, uint32_t len) {
  SWlrTlv  stackTlvs[WLR_SCHEMA_CHECK_STACK_TLVS];
  SWlrTlv *tlvs = stackTlvs;
  int32_t  count = 0;
  int32_t  capacity;

  if (wlrDecodeTlvs(payload, len, NULL, 0, &count) != WLR_DECODE_OK) {
    tscError("WLR: encoded payload for api:%u phase:%u does not decode", apiType, phase);
    return false;
  }
  if (count > WLR_SCHEMA_CHECK_STACK_TLVS) {
    if ((uint32_t)count > WLR_MAX_FRAME_PAYLOAD / WLR_TLV_HEADER_SIZE) {
      tscError("WLR: schema-check TLV count exceeds frame capacity for api:%u phase:%u", apiType,
               phase);
      return false;
    }
    tlvs = (SWlrTlv *)taosMemoryMalloc((size_t)count * sizeof(SWlrTlv));
    if (tlvs == NULL) {
      tscError("WLR: cannot allocate schema-check table for api:%u phase:%u, tlvs:%d", apiType,
               phase, count);
      return false;
    }
  }
  capacity = count;
  if (wlrDecodeTlvs(payload, len, tlvs, capacity, &count) != WLR_DECODE_OK) {
    tscError("WLR: encoded payload for api:%u phase:%u does not decode", apiType, phase);
    if (tlvs != stackTlvs) taosMemoryFree(tlvs);
    return false;
  }
  bool valid = wlrValidateRequiredTags(WLR_FRAME_EVENT, apiType, phase, tlvs, count) == WLR_DECODE_OK;
  if (!valid) {
    tscError("WLR: api:%u phase:%u missing a required tag; replay may reject this event", apiType,
             phase);
  }
  if (tlvs != stackTlvs) taosMemoryFree(tlvs);
  return valid;
}

int32_t wlrApiBegin(const SWorkloadBeginArgs *args, SWorkloadCallToken *token) {
  SWlrSlot *slot = NULL;
  uint8_t  *buf = NULL;
  int32_t   off = 0;
  int32_t   cap;
  uint32_t  bound;
  uint64_t  callId;
  int64_t   wallNs;
  int64_t   monoNs;
  int32_t   code;
  bool      metaOnly = false; /* payload too large for one frame: fixed tags only */
  int32_t   offRequired = 0;  /* offset just past the required common tags */

  if (token == NULL) return WLR_E_CONFIG;
  memset(token, 0, sizeof(*token));
  if (args == NULL) return WLR_E_CONFIG;

  /* Hot path when recording is off: a single atomic load, no allocation, no
   * string work (DS 9.1). */
  if (atomic_load_32(&wlrMgr.state) != WLR_STATE_RUNNING) return WLR_E_WRITE;

  /* Outermost-only. Note this is per thread: an internal call made on a driver
   * completion thread sees depth 0 and WOULD be recorded, which depth cannot fix.
   * Such paths must call an internal *Impl() instead of the public entry point
   * (TS RE014). */
  if (wlrTlsDepth > 0) {
    token->recorded = false;
    return WLR_E_OK; /* nested: intentionally not recorded */
  }

  wlrTlsDepth++;
  token->depthOwner = true;

  wallNs = taosGetTimestampNs();
  monoNs = taosGetMonoTimestampNs();
  callId = (uint64_t)atomic_add_fetch_64(&wlrMgr.nextCallId, 1);

  bound = wlrBeginPayloadBound(args);
  if (bound > WLR_MAX_FRAME_PAYLOAD) {
    /* Chunked streaming for oversize payloads is a separate path; until it lands
     * such a call is recorded without its payload rather than dropped silently,
     * and the omission is logged.
     *
     * metaOnly is what makes that true. Shrinking the reservation alone is not
     * enough: the variable-length fields below would still be attempted, the first
     * one would fail, and off would collapse to 0 -- publishing an EMPTY payload
     * and tripping the schema check on every required tag. The flag skips those
     * fields so the fixed metadata actually fits and is recorded. */
    tscWarn("WLR: BEGIN payload %u exceeds frame limit for api:%u, recording metadata only", bound,
            args->apiType);
    bound = WLR_TLV_FIXED_MAX;
    metaOnly = true;
  }

  code = wlrReserve(&wlrMgr, bound, WLR_FRAME_EVENT, callId, &slot, &buf);
  if (code != WLR_E_OK) {
    wlrTlsDepth--;
    token->depthOwner = false;
    return code;
  }

  cap = (int32_t)bound;

  /* Common required tags, in the order the validate table expects. */
  off = wlrPutTlvU64(buf, cap, off, WLR_TAG_WALL_NS, (uint64_t)wallNs);
  if (off > 0) off = wlrPutTlvU64(buf, cap, off, WLR_TAG_MONO_NS, (uint64_t)monoNs);
  if (off > 0) off = wlrPutTlvU64(buf, cap, off, WLR_TAG_ORIGIN_TID, (uint64_t)taosGetSelfPthreadId());
  if (off > 0) off = wlrPutTlvU64(buf, cap, off, WLR_TAG_API_TYPE, args->apiType);
  if (off > 0) off = wlrPutTlvU64(buf, cap, off, WLR_TAG_EVENT_PHASE, WLR_PHASE_BEGIN);
  if (off > 0) off = wlrPutTlvU64(buf, cap, off, WLR_TAG_CALL_ID, callId);

  bool carriesSql = args->apiType == WLR_API_QUERY || args->apiType == WLR_API_QUERY_WITH_REQID ||
                    args->apiType == WLR_API_QUERY_A || args->apiType == WLR_API_QUERY_A_WITH_REQID ||
                    args->apiType == WLR_API_VALIDATE_SQL || args->apiType == WLR_API_STMT_PREPARE ||
                    args->apiType == WLR_API_STMT2_PREPARE;

  /* Oversized calls are recorded as metadata-only, but required schema tags must
   * still be present so the event remains structurally valid. Empty required text
   * explicitly means the original payload was unavailable. */
  if (off > 0 && metaOnly) {
    if (carriesSql) {
      off = wlrPutTlvRaw(buf, cap, off, WLR_TAG_SQL, true, NULL, 0);
    /* CONNECT_TOKEN has no required USER tag; the common tags above already satisfy
     * its meta-only schema. */
    } else if (args->apiType == WLR_API_CONNECT || args->apiType == WLR_API_CONNECT_WITH ||
               args->apiType == WLR_API_CONNECT_AUTH || args->apiType == WLR_API_CONNECT_TOTP) {
      off = wlrPutTlvRaw(buf, cap, off, WLR_TAG_USER, true, NULL, 0);
    } else if (args->apiType == WLR_API_OPTIONS_CONNECTION && args->connOptSet) {
      off = wlrPutTlvU64(buf, cap, off, WLR_TAG_CONN_OPT_TYPE, (uint64_t)args->connOptType);
      if (off > 0) {
        off = wlrPutTlvRaw(buf, cap, off, WLR_TAG_CONN_OPT_NAME, true, NULL, 0);
      }
    } else if (args->apiType == WLR_API_SELECT_DB) {
      off = wlrPutTlvRaw(buf, cap, off, WLR_TAG_DB, true, NULL, 0);
    }
  }

  /* Async parent, when this call was issued from inside a recorded callback. Zero
   * means "no parent"; a stale non-zero value here would make replay wait on an
   * unrelated dependency (DS 6.8). */
  if (off > 0 && wlrTlsCallbackParent != 0) {
    off = wlrPutTlvU64(buf, cap, off, WLR_TAG_PARENT_CALL_ID, wlrTlsCallbackParent);
  }

  if (off > 0 && args->logicalConnId != 0) {
    off = wlrPutTlvU64(buf, cap, off, WLR_TAG_CONN_ID, args->logicalConnId);
  }
  if (off > 0 && args->logicalStmtId != 0) {
    off = wlrPutTlvU64(buf, cap, off, WLR_TAG_STMT_ID, args->logicalStmtId);
  }
  if (off > 0 && args->apiType == WLR_API_STMT2_EXEC && args->stmt2Async) {
    off = wlrPutTlvU64(buf, cap, off, WLR_TAG_STMT2_ASYNC, 1);
  }
  if (off > 0 && args->stmtInitOptionsSet) {
    off = wlrPutTlvU64(buf, cap, off, WLR_TAG_STMT_INIT_OPTIONS, args->stmtInitOptions);
  }
  if (off > 0 && args->logicalResultId != 0) {
    off = wlrPutTlvU64(buf, cap, off, WLR_TAG_RESULT_ID, args->logicalResultId);
  }
  if (off > 0 && args->reqid != 0) {
    off = wlrPutTlvU64(buf, cap, off, WLR_TAG_REQID, (uint64_t)args->reqid);
  }
  if (off > 0) offRequired = off;

  /* SQL is stored as plain UTF-8 with an explicit length. It is deliberately not
   * masked: a masked statement cannot be replayed (FS 6.3). */
  if (off > 0 && !metaOnly && carriesSql) {
    bool omitted = false;
    off = wlrPutRequiredBytesOrEmpty(buf, cap, off, WLR_TAG_SQL, args->sql,
                                     args->sqlLen > 0 ? (uint32_t)args->sqlLen : 0, &omitted);
    if (omitted) {
      tscWarn("WLR: required SQL was unavailable or did not fit for api:%u; "
              "recording an empty required tag",
              args->apiType);
    }
    if (off > 0) offRequired = off;
  }

  /* Connect: endpoint/user/db/port only. Password, token and TOTP are never read
   * by the recorder at all, so they cannot reach the file (FS 6.2). */
  /* These are counted inside WLR_TLV_FIXED_MAX for a normal reservation, but they
   * are still caller-supplied strings: under metaOnly the budget is only 512 bytes,
   * so a long endpoint or db name would collapse off and cost the required tags. */
  bool isConnect = args->apiType == WLR_API_CONNECT || args->apiType == WLR_API_CONNECT_WITH ||
                   args->apiType == WLR_API_CONNECT_AUTH || args->apiType == WLR_API_CONNECT_TOTP ||
                   args->apiType == WLR_API_CONNECT_TOKEN;
  bool carriesDb = isConnect || args->apiType == WLR_API_SELECT_DB;
  if (off > 0 && !metaOnly && carriesDb) {
    if (isConnect && args->apiType != WLR_API_CONNECT_TOKEN) {
      bool omitted = false;
      uint32_t userLen = args->user != NULL ? (uint32_t)strlen(args->user) : 0;
      off = wlrPutRequiredBytesOrEmpty(buf, cap, off, WLR_TAG_USER, args->user, userLen, &omitted);
      if (omitted) {
        tscWarn("WLR: required user was unavailable or did not fit for api:%u; "
                "recording an empty required tag",
                args->apiType);
      }
      if (off > 0) offRequired = off;
    }
    if (isConnect && off > 0) off = wlrPutTlvStr(buf, cap, off, WLR_TAG_ENDPOINT, args->endpoint);
    if (off > 0 && isConnect) {
      off = wlrPutTlvStr(buf, cap, off, WLR_TAG_DB, args->db);
    } else if (off > 0) {
      bool omitted = false;
      uint32_t dbLen = args->db != NULL ? (uint32_t)strlen(args->db) : 0;
      off = wlrPutRequiredBytesOrEmpty(buf, cap, off, WLR_TAG_DB, args->db, dbLen, &omitted);
    }
    if (isConnect && off > 0 && args->port != 0) {
      off = wlrPutTlvU64(buf, cap, off, WLR_TAG_PORT, args->port);
    }
  }

  /* Connection option: name and type always, value only when the caller supplied
   * one. A NULL value is a legal call meaning "clear this option", and its absence
   * is how the file represents that -- an empty string would be a different fact. */
  if (off > 0 && !metaOnly && args->connOptSet) {
    off = wlrPutTlvU64(buf, cap, off, WLR_TAG_CONN_OPT_TYPE, (uint64_t)args->connOptType);
    if (off > 0) {
      int32_t nameOffset = off;
      bool omitted = false;
      uint32_t nameLen = args->connOptName != NULL ? (uint32_t)strlen(args->connOptName) : 0;
      off = wlrPutRequiredBytesOrEmpty(buf, cap, off, WLR_TAG_CONN_OPT_NAME, args->connOptName,
                                       nameLen, &omitted);
      if (omitted) {
        tscWarn("WLR: required connection option name was unavailable or did not fit; "
                "recording an empty required tag");
      }
      if (off > 0 && args->connOptValue != NULL) {
        if (wlrConnOptValueSafe(args->connOptType)) {
          off = wlrPutTlvRaw(buf, cap, off, WLR_TAG_CONN_OPT_VALUE, true, args->connOptValue,
                             (uint32_t)strlen(args->connOptValue));
        } else {
          off = -1;
        }
        if (off < 0) {
          tscWarn("WLR: connection option value unavailable; recording an empty name");
          wlrPutU32(buf + nameOffset + WLR_TLV_OFF_LENGTH, 0);
          off = nameOffset + WLR_TLV_HEADER_SIZE;
        }
      }
      if (off > 0) offRequired = off;
    }
  }

  /* Option SET for taos_connect_with. This is an optional 1.0 tag, so readers skip
   * it by length when it is not part of the schema they act on (DS 11.3). */
  if (off > 0 && !metaOnly && args->connOptList != NULL && args->connOptListLen > 0) {
    off = wlrPutTlvRaw(buf, cap, off, WLR_TAG_CONN_OPT_LIST, false, args->connOptList,
                       (uint32_t)args->connOptListLen);
  } else if (off > 0 && args->connOptListOmitted) {
    /* A valid encoded list always contains at least its count field. Zero length
     * is therefore an unambiguous marker that capture omitted a non-empty list. */
    off = wlrPutTlvRaw(buf, cap, off, WLR_TAG_CONN_OPT_LIST, false, NULL, 0);
  }

  const bool isSml = wlrApiIsSml(args->apiType);
  if (off > 0 && isSml) {
    /* The metadata-only reservation is sized for all common and SML scalar tags.
     * Variable payloads remain excluded so future additions cannot displace the
     * required prefix; wlrCheckSchema below enforces that invariant. */
    off = wlrPutTlvU64(buf, cap, off, WLR_TAG_SML_PROTOCOL, (uint64_t)args->smlProtocol);
    if (off > 0) off = wlrPutTlvU64(buf, cap, off, WLR_TAG_SML_PRECISION, (uint64_t)args->smlPrecision);
    if (off > 0 && args->smlTtl != 0) {
      off = wlrPutTlvU64(buf, cap, off, WLR_TAG_SML_TTL, (uint64_t)args->smlTtl);
    }
    if (off > 0 && args->smlCount > 0) {
      off = wlrPutTlvU64(buf, cap, off, WLR_TAG_SML_COUNT, (uint64_t)args->smlCount);
    }
    if (off > 0) offRequired = off;
    if (off > 0 && !metaOnly && args->smlTbnameKey != NULL) {
      off = wlrPutTlvRaw(buf, cap, off, WLR_TAG_SML_TBNAME_KEY, false, args->smlTbnameKey,
                         (uint32_t)strlen(args->smlTbnameKey));
    }
    /* Raw variant: one contiguous buffer, stored verbatim. Line variant: N separate
     * strings, stored as count + per-entry lengths + bytes so the boundaries
     * survive (DS 6.10). Both land in SML_PAYLOAD; the API type says which shape it
     * is, and only one is ever set. */
    if (off > 0 && !metaOnly && args->smlRaw != NULL && args->smlRawLen > 0) {
      off = wlrPutTlvRaw(buf, cap, off, WLR_TAG_SML_PAYLOAD, false, args->smlRaw,
                         (uint32_t)args->smlRawLen);
    } else if (off > 0 && !metaOnly && args->smlLines != NULL && args->smlCount > 0) {
      uint32_t need = wlrSmlLinesEncodedSize(args->smlLines, args->smlCount);
      uint32_t remaining = (off >= 0 && off <= cap) ? (uint32_t)(cap - off) : 0;
      if (need != UINT32_MAX && off + WLR_TLV_HEADER_SIZE <= cap &&
          remaining >= WLR_TLV_HEADER_SIZE &&
          need <= remaining - WLR_TLV_HEADER_SIZE) {
        int32_t n = wlrEncodeSmlLines(buf + off + WLR_TLV_HEADER_SIZE,
                                      (size_t)(remaining - WLR_TLV_HEADER_SIZE), args->smlLines,
                                      args->smlCount);
        if (n > 0) {
          /* Write the TLV header around the payload encoded in place, avoiding a
           * second copy of what may be a large batch of lines. */
          wlrPutU16(buf + off + WLR_TLV_OFF_TAG, WLR_TAG_SML_PAYLOAD);
          wlrPutU16(buf + off + WLR_TLV_OFF_FLAGS, 0);
          wlrPutU32(buf + off + WLR_TLV_OFF_LENGTH, (uint32_t)n);
          off += WLR_TLV_HEADER_SIZE + n;
        } else {
          tscWarn("WLR: failed to encode schemaless line payload, keeping required tags only");
          off = -1;
        }
      } else {
        tscWarn("WLR: schemaless line payload %u exceeds frame space, keeping required tags only", need);
        off = -1;
      }
    }
  }
  /* STMT binds arrive already described column-wise. Raw TAOS_MULTI_BIND /
   * TAOS_STMT2_BIND memory must never be serialized: it holds pointers and
   * platform padding (DS 6.9). */
  if (off > 0 && !metaOnly && args->bindDesc != NULL && args->bindDescLen > 0) {
    off = wlrPutTlvBytes(buf, cap, off, WLR_TAG_BIND_BYTES, args->bindDesc, args->bindDescLen);
    if (off < 0) {
      tscWarn("WLR: bind payload length:%d exceeds reservation for api:%u", args->bindDescLen,
              args->apiType);
    }
  }
  /* Column index, when the call named one. Optional and written unrequired so a
   * reader that does not know the tag skips it by length (DS 11.3). Cast through
   * uint64: the value may be -1 for all-columns, and the reader sign-extends it back. */
  /* All required bind tags end with BIND_BYTES. Optional shape metadata below is
   * omitted in metadata-only mode and must never move the fallback boundary. */
  if (off > 0) offRequired = off;
  if (off > 0 && !metaOnly && args->bindColIdxSet) {
    off = wlrPutTlvU64Optional(buf, cap, off, WLR_TAG_BIND_COL_IDX,
                               (uint64_t)(int64_t)args->bindColIdx);
  }
  if (off > 0 && !metaOnly && args->bindTableCountSet) {
    off = wlrPutTlvU64Optional(buf, cap, off, WLR_TAG_BIND_COUNT,
                               (uint64_t)(int64_t)args->bindTableCount);
  }
  if (off > 0 && !metaOnly && args->tbnameList != NULL && args->tbnameListLen > 0) {
    off = wlrPutTlvRaw(buf, cap, off, WLR_TAG_STMT2_NULLABLE_TBNAMES, true, args->tbnameList,
                       (uint32_t)args->tbnameListLen);
  }
  if (off > 0 && !metaOnly && args->stmt2Tags != NULL && args->stmt2TagsLen > 0) {
    off = wlrPutTlvRaw(buf, cap, off, WLR_TAG_STMT2_TAGS, true, args->stmt2Tags,
                       (uint32_t)args->stmt2TagsLen);
  }
  if (off > 0 && !metaOnly && args->tbname != NULL && args->tbname[0] != 0) {
    off = wlrPutTlvRaw(buf, cap, off, WLR_TAG_STMT_TBNAME, false, args->tbname,
                       (uint32_t)strlen(args->tbname));
  }


  if (off <= 0) {
    if (offRequired <= 0) {
      tscError("WLR: BEGIN payload has no valid required prefix for api:%u", args->apiType);
      slot->payloadLen = 0;
      (void)wlrPublish(&wlrMgr, slot, false);
      token->recorded = false;
      wlrTlsDepth--;
      token->depthOwner = false;
      return WLR_E_WRITE;
    }
    /* Encoding overran the reservation. Publish what fits rather than abandoning
     * the slot: the writer walks sequence numbers strictly, so a hole would stall
     * it permanently.
     *
     * Fall back to the required-tag prefix rather than to zero. An empty payload
     * would fail the schema check on every required tag and give replay nothing at
     * all. offRequired is advanced after each API-specific required tag; the schema
     * check below rejects the slot if a future bound regression breaks that invariant. */
    tscWarn("WLR: BEGIN payload encode overflow for api:%u, keeping %d required bytes",
            args->apiType, offRequired);
    off = offRequired;
  }
  slot->payloadLen = (uint32_t)off;
  if (!wlrCheckSchema(args->apiType, WLR_PHASE_BEGIN, buf, slot->payloadLen)) {
    atomic_store_32(&wlrMgr.captureIncomplete, 1);
    slot->payloadLen = 0;
    (void)wlrPublish(&wlrMgr, slot, false);
    token->recorded = false;
    wlrTlsDepth--;
    token->depthOwner = false;
    return WLR_E_WRITE;
  }

  token->callId = callId;
  token->eventSeq = slot->eventSeq;
  token->beginMonoNs = monoNs;
  token->apiType = args->apiType;
  token->recorded = true;

  /* Registered before the ack wait, so a call that hangs inside the real API is
   * already on the unfinished list when the session stops (DS 6.4). */
  wlrActiveAdd(&wlrMgr, callId);

  /* Wait for the BEGIN frame to reach the file before the real API runs. This is
   * the property that makes a hung call diagnosable (DS 6.14). */
  code = wlrPublish(&wlrMgr, slot, /*waitAck=*/true);
  if (code != WLR_E_OK) {
    /* Ack failed: recording of this call is not guaranteed, but the business call
     * must still proceed (FS 4.8).
     *
     * The active-list entry added above must come back off here. Clearing recorded
     * is what makes wlrApiReturn skip its completion work, and that skip includes
     * wlrActiveRemove -- so leaving the entry would strand it in the fixed ring and
     * report this call as unfinished in SESSION_STOP even though it completed. */
    wlrActiveRemove(&wlrMgr, callId);
    token->recorded = false;
    wlrTlsDepth--;
    token->depthOwner = false;
    return code;
  }

  return WLR_E_OK;
}

void wlrApiSkipToken(SWorkloadCallToken *token) {
  if (token != NULL) memset(token, 0, sizeof(*token));
}

static void wlrEmitCompletion(SWorkloadCallToken *token, uint16_t phase, int32_t retCode,
                              uint64_t connId, uint64_t stmtId, uint64_t resultId, int64_t affected,
                              int64_t fetched, int64_t qid) {
  SWlrSlot *slot = NULL;
  uint8_t  *buf = NULL;
  int32_t   off = 0;
  int32_t   offRequired = 0; /* offset just past the required common tags */
  int32_t   cap = WLR_TLV_FIXED_MAX;
  int64_t   nowMono = taosGetMonoTimestampNs();

  if (atomic_load_32(&wlrMgr.state) != WLR_STATE_RUNNING) return;

  if (wlrReserve(&wlrMgr, (uint32_t)cap, WLR_FRAME_EVENT, token->callId, &slot, &buf) !=
      WLR_E_OK) {
    return;
  }

  off = wlrPutTlvU64(buf, cap, off, WLR_TAG_WALL_NS, (uint64_t)taosGetTimestampNs());
  if (off > 0) off = wlrPutTlvU64(buf, cap, off, WLR_TAG_MONO_NS, (uint64_t)nowMono);
  if (off > 0) off = wlrPutTlvU64(buf, cap, off, WLR_TAG_ORIGIN_TID, (uint64_t)taosGetSelfPthreadId());
  if (off > 0) off = wlrPutTlvU64(buf, cap, off, WLR_TAG_API_TYPE, token->apiType);
  if (off > 0) off = wlrPutTlvU64(buf, cap, off, WLR_TAG_EVENT_PHASE, phase);
  if (off > 0) off = wlrPutTlvU64(buf, cap, off, WLR_TAG_CALL_ID, token->callId);

  /* Signed values go through (uint64_t)(int64_t): widen to 64 bits FIRST, so the
   * stored value is the two's-complement bit pattern of the full int64 and a reader
   * recovers it by casting straight back. This applies to every signed field below --
   * RET_CODE (routinely negative: TSDB_CODE_* errors), QID, AFFECTED_ROWS and
   * FETCH_ROWS -- and is the same convention BIND_COL_IDX documents for its -1
   * all-columns sentinel. taosReplay reads RET_CODE as (int32_t)(int64_t)wlrGetU64,
   * which is the exact inverse.
   *
   * Casting a negative int32_t directly to uint64_t would zero-extend from 32 bits
   * and store e.g. -1 as 0x00000000ffffffff instead of 0xffffffffffffffff, so the
   * intermediate int64_t is load-bearing rather than stylistic. */
  if (off > 0) off = wlrPutTlvU64(buf, cap, off, WLR_TAG_RET_CODE, (uint64_t)(int64_t)retCode);
  if (off > 0 && connId != 0) off = wlrPutTlvU64(buf, cap, off, WLR_TAG_CONN_ID, connId);
  if (off > 0 && stmtId != 0) off = wlrPutTlvU64(buf, cap, off, WLR_TAG_STMT_ID, stmtId);
  if (off > 0 && resultId != 0) off = wlrPutTlvU64(buf, cap, off, WLR_TAG_RESULT_ID, resultId);
  /* Connect RETURN deliberately has no required CONN_ID: a failed connection has
   * no logical ID to attach, so validation keeps these events outcome-only. */
  if (off > 0) {
    off = wlrPutTlvU64(buf, cap, off, WLR_TAG_AFFECTED_ROWS, (uint64_t)(int64_t)affected);
  }
  if (off > 0) off = wlrPutTlvU64(buf, cap, off, WLR_TAG_FETCH_ROWS, (uint64_t)(int64_t)fetched);
  if (off > 0 && qid != 0) off = wlrPutTlvU64(buf, cap, off, WLR_TAG_QID, (uint64_t)(int64_t)qid);
  offRequired = (off > 0) ? off : 0;

  /* Duration lets replay compare against the original without storing results.
   *
   * Clamped at zero rather than written raw: the subtraction is signed, and the
   * cast to uint64_t would turn any negative result into a duration near 2^64.
   * A monotonic clock should never go backwards, but a token whose beginMonoNs was
   * stamped in a different clock domain (a DONE rebuilt on a driver thread) or a
   * clock reset would produce one, and an absurd duration in the trace is worse
   * than a zero -- replay compares these values. */
  if (off > 0 && token->beginMonoNs != 0 && nowMono >= token->beginMonoNs) {
    off = wlrPutTlvU64(buf, cap, off, WLR_TAG_DURATION_NS, (uint64_t)(nowMono - token->beginMonoNs));
  }

  /*
   * Fall back to the required-tag prefix, mirroring wlrApiBegin, rather than publishing
   * a zero-length payload.
   *
   * An empty completion frame fails every required tag in wlrCheckSchema and gives
   * replay nothing to match the call against -- strictly worse than a valid partial
   * event that carries the outcome. Unreachable today (WLR_TLV_FIXED_MAX is 512 and the
   * fixed tags run ~112 bytes), but adding one variable-length tag here would make it
   * reachable, and the failure would be silent.
   */
  if (off <= 0) {
    if (offRequired > 0) {
      tscWarn("WLR: completion payload encode overflow for api:%u phase:%u, keeping %d required bytes",
              token->apiType, phase, offRequired);
      off = offRequired;
    } else {
      tscError("WLR: completion payload has no valid required prefix for api:%u phase:%u",
               token->apiType, phase);
      slot->payloadLen = 0;
      (void)wlrPublish(&wlrMgr, slot, false);
      return;
    }
  }
  slot->payloadLen = (uint32_t)off;
  (void)wlrCheckSchema(token->apiType, phase, buf, slot->payloadLen);

  /* Completions do not wait for a write ack: only BEGIN needs that guarantee, and
   * acking every event would put a disk round trip in every API call (DS 9.2). */
  (void)wlrPublish(&wlrMgr, slot, /*waitAck=*/false);
}

void wlrApiReturn(SWorkloadCallToken *token, const SWorkloadReturnArgs *args) {
  if (token == NULL) return;

  /* Depth must unwind even when the call was not recorded or the ack failed,
   * otherwise this thread would never record again (TS RE016). */
  if (token->depthOwner) {
    if (wlrTlsDepth > 0) wlrTlsDepth--;
    token->depthOwner = false;
  }

  if (!token->recorded) return;

  /* Removed before the args check, not after: a recorded call that reaches here has
   * finished, whatever the caller chose to report about it. Folding the two
   * conditions into one guard stranded the ID in the active ring when args was
   * NULL, and SESSION_STOP would then list a completed call as unfinished -- the
   * opposite of what that list is for. Every call site currently pairs NULL args
   * with an unrecorded token, so this is not reachable today; the split keeps it
   * from becoming reachable. */
  wlrActiveRemove(&wlrMgr, token->callId);

  if (args == NULL) return;

  wlrEmitCompletion(token, WLR_PHASE_RETURN, args->code, args->logicalConnId, args->logicalStmtId,
                    args->logicalResultId, args->affectedRows, args->fetchRows, args->qid);
}

void wlrAsyncDone(SWorkloadCallToken *token, const SWorkloadDoneArgs *args) {
  if (token == NULL || !token->recorded || args == NULL) return;

  /* DONE may arrive on a driver thread, long after RETURN unwound the depth on
   * the originating thread. It must not touch depth at all. */
  wlrEmitCompletion(token, WLR_PHASE_DONE, args->code, 0, args->logicalStmtId, args->logicalResultId, args->affectedRows,
                    args->fetchRows, 0);
}

void wlrResultFree(uint64_t logicalResultId) {
  /* Intentional no-op: result IDs are currently resolved from the event stream, so
   * there is no in-memory mapping to erase. Update this hook before introducing
   * stateful result-ID cleanup. */
  (void)logicalResultId;
}

void wlrConnectComplete(SWorkloadCallToken *token, uint64_t *logicalConnId) {
  if (token == NULL || logicalConnId == NULL) return;
  /* token must be initialized by wlrApiBegin (or a BEGIN macro), including for
   * skipped nested calls, because wlrApiReturn owns its depthOwner bookkeeping. */
  /* Nested taos_connect inside taos_connect_with is intentionally unrecorded, but
   * it must still mint the connection ID reported by the outer recorded call. */
  if (token->recorded || wlrRecorderState() == WLR_STATE_RUNNING) {
    *logicalConnId = wlrNextConnId();
  }
  if (token->recorded) {
    SWorkloadReturnArgs ret = {.code = TSDB_CODE_SUCCESS, .logicalConnId = *logicalConnId};
    wlrApiReturn(token, &ret);
  } else {
    wlrApiReturn(token, NULL);
  }
}
