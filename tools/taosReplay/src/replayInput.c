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
 * taosreplay input handling (DS 6.17).
 *
 * Recording files are untrusted input: they may come from a customer site, have been
 * copied while being written, or be truncated by a full disk. Every length is
 * checked by the shared decoder, and this file treats a decode failure as data to
 * report rather than a reason to abort.
 *
 * The distinction that governs everything here is torn tail vs mid-file damage. A
 * copy of an active segment legitimately ends mid-frame; that prefix is fully usable
 * and must not be reported as corruption, or the copy-anytime property that makes
 * this feature usable in production would be lost (FS 4.9).
 */

#include "replayInt.h"

#define REPLAY_MAX_SEGMENT_BYTES (1ll << 30)
#define REPLAY_MAX_TOTAL_SEGMENT_BYTES (4ll << 30)

static bool replayHasParentComponent(const char *path) {
  const char *p = path;
  while (*p != '\0') {
    while (*p == '/' || *p == '\\') ++p;
    const char *start = p;
    while (*p != '\0' && *p != '/' && *p != '\\') ++p;
    if ((size_t)(p - start) == 2 && start[0] == '.' && start[1] == '.') return true;
  }
  return false;
}

static int32_t replayReadFile(const char *path, SReplayBuffer *out) {
  int64_t   size = 0;
  TdFilePtr f = NULL;
  int64_t   total = 0;

  if (taosStatFile(path, &size, NULL, NULL) != 0) {
    (void)fprintf(stderr, "%s: stat failed: %s (code %d)\n", path, tstrerror(terrno), terrno);
    return -1;
  }
  if (size <= 0) {
    (void)fprintf(stderr, "%s: segment is empty\n", path);
    return -1;
  }
  if (size > REPLAY_MAX_SEGMENT_BYTES) {
    (void)fprintf(stderr, "%s: segment is too large (%" PRId64 " bytes)\n", path, size);
    return -1;
  }
#if SIZE_MAX < INT64_MAX
  if ((uint64_t)size > (uint64_t)SIZE_MAX) {
    (void)fprintf(stderr, "%s: segment is too large for this platform (%" PRId64 " bytes)\n", path,
                  size);
    return -1;
  }
#endif
  f = taosOpenFile(path, TD_FILE_READ);
  if (f == NULL) {
    (void)fprintf(stderr, "%s: open failed: %s (code %d)\n", path, tstrerror(terrno), terrno);
    return -1;
  }

  out->buf = (uint8_t *)taosMemoryMalloc((size_t)size);
  if (out->buf == NULL) {
    (void)fprintf(stderr, "%s: unable to allocate %" PRId64 " bytes\n", path, size);
    (void)taosCloseFile(&f);
    return -1;
  }
  while (total < size) {
    int64_t got = taosReadFile(f, out->buf + (size_t)total, size - total);
    if (got < 0) {
      (void)fprintf(stderr, "%s: read failed after %" PRId64 " bytes: %s (code %d)\n", path,
                    total, tstrerror(terrno), terrno);
      break;
    }
    if (got == 0) {
      (void)fprintf(stderr, "%s: unexpected EOF after %" PRId64 " of %" PRId64 " bytes\n",
                    path, total, size);
      break;
    }
    total += got;
  }
  (void)taosCloseFile(&f);

  if (total != size) {
    taosMemoryFreeClear(out->buf);
    out->len = 0;
    return -1;
  }
  out->len = size;
  return 0;
}

static int32_t replayEnsureCap(SReplaySession *s) {
  if (s->eventCount < s->eventCap) return 0;
  if (s->eventCap >= REPLAY_MAX_EVENTS) return -1;
  int64_t proposed = (s->eventCap == 0) ? 4096 : (int64_t)s->eventCap * 2;
  int32_t newCap = proposed > REPLAY_MAX_EVENTS ? REPLAY_MAX_EVENTS : (int32_t)proposed;
  SReplayEvent *n = (SReplayEvent *)taosMemoryRealloc(s->events, (size_t)newCap * sizeof(SReplayEvent));
  if (n == NULL) return -1;
  s->events = n;
  s->eventCap = newCap;
  return 0;
}

/*
 * Read a u64-valued TLV defensively.
 *
 * wlrGetU64 always reads 8 bytes, and wlrDecodeTlvs reports value == NULL for a
 * zero-length TLV while still returning WLR_DECODE_OK. replayLoadSegment does not run
 * wlrValidateRequiredTags before filling events, so a recording carrying a short or
 * empty numeric tag reaches here -- and recordings are explicitly untrusted input. Left
 * unguarded this was a NULL dereference (or an 8-byte overread past a 1-byte tag).
 *
 * wlrDecodeTlvs has already proved that the declared value range stays inside the
 * payload. Requiring exactly 8 bytes here additionally prevents a numeric field from
 * accepting an overlong value with ambiguous trailing data. A malformed tag yields 0
 * and leaves the field at its memset default, which downstream treats as not recorded.
 */
static bool replayTlvU64(const SWlrTlv *tlv, uint64_t *out) {
  if (tlv->value == NULL || tlv->length != sizeof(uint64_t)) return false;
  *out = wlrGetU64(tlv->value);
  return true;
}

static int64_t replaySignedU64(uint64_t value) {
  int64_t signedValue;
  memcpy(&signedValue, &value, sizeof(signedValue));
  return signedValue;
}

/* Pull the fields replay needs out of one event's TLV payload. */
static bool replayFillEvent(SReplayEvent *ev, const SWlrTlv *tlvs, int32_t count) {
  /* String pointers below borrow the segment buffer and are length-delimited,
   * not NUL-terminated C strings. Consumers must always honor the matching
   * length field when passing them to APIs or formatting them. The caller has
   * already run wlrDecodeTlvs, which bounds every TLV length by its payload. */
  int32_t i;
  bool valid = true;
  for (i = 0; i < count; ++i) {
    uint16_t tag = WLR_TAG_BASE(tlvs[i].tag);
    uint64_t v = 0;
    switch (tag) {
      case WLR_TAG_WALL_NS:
        if (replayTlvU64(&tlvs[i], &v)) ev->wallNs = replaySignedU64(v);
        else valid = false;
        break;
      case WLR_TAG_MONO_NS:
        if (replayTlvU64(&tlvs[i], &v)) {
          ev->monoNs = replaySignedU64(v);
          ev->haveMonoNs = true;
        } else valid = false;
        break;
      case WLR_TAG_ORIGIN_TID:
        if (replayTlvU64(&tlvs[i], &v)) ev->originTid = v;
        else valid = false;
        break;
      case WLR_TAG_API_TYPE:
        if (replayTlvU64(&tlvs[i], &v) && v <= UINT16_MAX) ev->apiType = (uint16_t)v;
        else valid = false;
        break;
      case WLR_TAG_EVENT_PHASE:
        if (replayTlvU64(&tlvs[i], &v) && v <= UINT16_MAX) ev->phase = (uint16_t)v;
        else valid = false;
        break;
      case WLR_TAG_CALL_ID:
        if (replayTlvU64(&tlvs[i], &v)) ev->callId = v;
        else valid = false;
        break;
      case WLR_TAG_PARENT_CALL_ID:
        if (replayTlvU64(&tlvs[i], &v)) ev->parentCallId = v;
        else valid = false;
        break;
      case WLR_TAG_CONN_ID:
        if (replayTlvU64(&tlvs[i], &v)) ev->connId = v;
        else valid = false;
        break;
      case WLR_TAG_STMT_ID:
        if (replayTlvU64(&tlvs[i], &v)) ev->stmtId = v;
        else valid = false;
        break;
      case WLR_TAG_STMT2_ASYNC:
        if (replayTlvU64(&tlvs[i], &v) && v == 1) ev->stmt2Async = true;
        else valid = false;
        break;
      case WLR_TAG_STMT_INIT_OPTIONS:
        if (replayTlvU64(&tlvs[i], &v) &&
            v <= (WLR_STMT_OPT_SINGLE_STB_INSERT | WLR_STMT_OPT_SINGLE_TABLE_BIND_ONCE |
                  WLR_STMT_OPT_ASYNC_CALLBACK | WLR_STMT_OPT_USERDATA)) {
          ev->stmtInitOptions = (uint8_t)v;
          ev->stmtInitOptionsSet = true;
        } else valid = false;
        break;
      case WLR_TAG_RESULT_ID:
        if (replayTlvU64(&tlvs[i], &v)) ev->resultId = v;
        else valid = false;
        break;
      case WLR_TAG_REQID:
        if (replayTlvU64(&tlvs[i], &v)) ev->reqid = replaySignedU64(v);
        else valid = false;
        break;
      case WLR_TAG_RET_CODE:
        if (replayTlvU64(&tlvs[i], &v)) {
          int64_t code = replaySignedU64(v);
          if (code < INT32_MIN || code > INT32_MAX) valid = false;
          else ev->retCode = (int32_t)code;
        } else valid = false;
        break;
      case WLR_TAG_AFFECTED_ROWS:
        if (replayTlvU64(&tlvs[i], &v)) {
          ev->affectedRows = replaySignedU64(v);
          ev->affectedRowsSet = true;
        } else valid = false;
        break;
      case WLR_TAG_FETCH_ROWS:
        if (replayTlvU64(&tlvs[i], &v)) {
          ev->fetchRows = replaySignedU64(v);
          ev->fetchRowsSet = true;
        } else valid = false;
        break;
      case WLR_TAG_SQL:
        if (tlvs[i].value != NULL && tlvs[i].length > 0) {
          ev->sql = (const char *)tlvs[i].value;
          ev->sqlLen = tlvs[i].length;
        }
        break;
      case WLR_TAG_STMT_TBNAME:
        if (tlvs[i].value != NULL && tlvs[i].length > 0) {
          ev->tbname = (const char *)tlvs[i].value;
          ev->tbnameLen = tlvs[i].length;
        }
        break;
      case WLR_TAG_BIND_BYTES:
        if (tlvs[i].value != NULL && tlvs[i].length > 0) {
          ev->bindBytes = tlvs[i].value;
          ev->bindLen = tlvs[i].length;
        }
        break;
      case WLR_TAG_BIND_COL_IDX:
        if (replayTlvU64(&tlvs[i], &v)) {
          int64_t colIdx = replaySignedU64(v);
          if (colIdx < INT32_MIN || colIdx > INT32_MAX) valid = false;
          else {
            ev->bindColIdx = (int32_t)colIdx;
            ev->bindColIdxSet = true;
          }
        } else valid = false;
        break;
      case WLR_TAG_BIND_COUNT:
        if (replayTlvU64(&tlvs[i], &v)) {
          int64_t tableCount = replaySignedU64(v);
          if (tableCount < 0 || tableCount > INT32_MAX) valid = false;
          else {
            ev->bindTableCount = (int32_t)tableCount;
            ev->bindTableCountSet = true;
          }
        } else valid = false;
        break;
      case WLR_TAG_STMT2_TBNAMES:
      case WLR_TAG_STMT2_NULLABLE_TBNAMES:
        if (tlvs[i].value != NULL && tlvs[i].length > 0) {
          ev->tbnameList = tlvs[i].value;
          ev->tbnameListLen = tlvs[i].length;
        }
        break;
      case WLR_TAG_STMT2_TAGS:
        if (tlvs[i].value != NULL && tlvs[i].length > 0) {
          ev->stmt2Tags = tlvs[i].value;
          ev->stmt2TagsLen = tlvs[i].length;
        }
        break;
      case WLR_TAG_ENDPOINT:
        if (tlvs[i].value != NULL && tlvs[i].length > 0) {
          ev->endpoint = (const char *)tlvs[i].value;
          ev->endpointLen = tlvs[i].length;
        }
        break;
      case WLR_TAG_PORT:
        if (replayTlvU64(&tlvs[i], &v) && v <= UINT16_MAX) ev->port = (uint16_t)v;
        else valid = false;
        break;
      case WLR_TAG_USER:
        if (tlvs[i].value != NULL && tlvs[i].length > 0) {
          ev->user = (const char *)tlvs[i].value;
          ev->userLen = tlvs[i].length;
        }
        break;
      case WLR_TAG_DB:
        if (tlvs[i].value != NULL && tlvs[i].length > 0) {
          ev->db = (const char *)tlvs[i].value;
          ev->dbLen = tlvs[i].length;
        }
        break;
      case WLR_TAG_CONN_OPT_LIST:
        if (tlvs[i].value != NULL && tlvs[i].length > 0) {
          ev->connOptList = tlvs[i].value;
          ev->connOptListLen = tlvs[i].length;
        } else if (tlvs[i].length == 0) {
          ev->connOptListOmitted = true;
        }
        break;
      case WLR_TAG_CONN_OPT_TYPE:
        if (replayTlvU64(&tlvs[i], &v)) {
          int64_t optType = replaySignedU64(v);
          if (optType < INT32_MIN || optType > INT32_MAX) valid = false;
          else {
            ev->connOptType = (int32_t)optType;
            ev->connOptTypeSet = true;
          }
        } else valid = false;
        break;
      case WLR_TAG_CONN_OPT_NAME:
        if (tlvs[i].value != NULL && tlvs[i].length > 0) {
          ev->connOptName = (const char *)tlvs[i].value;
          ev->connOptNameLen = tlvs[i].length;
        }
        break;
      case WLR_TAG_CONN_OPT_VALUE:
        /* Tag presence distinguishes an explicit empty string from CLEAR, whose
         * value tag is absent. wlrDecodeTlvs returns NULL for the former's bytes. */
        ev->connOptValue = (const char *)tlvs[i].value;
        ev->connOptValueLen = tlvs[i].length;
        ev->connOptValueSet = true;
        break;
      case WLR_TAG_SML_PROTOCOL:
        if (replayTlvU64(&tlvs[i], &v)) {
          int64_t protocol = replaySignedU64(v);
          if (protocol < INT32_MIN || protocol > INT32_MAX) valid = false;
          else ev->smlProtocol = (int32_t)protocol;
        } else valid = false;
        break;
      case WLR_TAG_SML_PRECISION:
        if (replayTlvU64(&tlvs[i], &v)) {
          int64_t precision = replaySignedU64(v);
          if (precision < INT32_MIN || precision > INT32_MAX) valid = false;
          else ev->smlPrecision = (int32_t)precision;
        } else valid = false;
        break;
      case WLR_TAG_SML_TTL:
        if (replayTlvU64(&tlvs[i], &v)) {
          int64_t ttl = replaySignedU64(v);
          if (ttl < INT32_MIN || ttl > INT32_MAX) valid = false;
          else ev->smlTtl = (int32_t)ttl;
        } else valid = false;
        break;
      case WLR_TAG_SML_TBNAME_KEY:
        ev->smlTbnameKey = (const char *)tlvs[i].value;
        ev->smlTbnameKeyLen = tlvs[i].length;
        ev->smlTbnameKeySet = true;
        break;
      case WLR_TAG_SML_PAYLOAD:
        if (tlvs[i].value != NULL && tlvs[i].length > 0) {
          ev->smlPayload = tlvs[i].value;
          ev->smlPayloadLen = tlvs[i].length;
        }
        break;
      default:
        /* Unknown optional tags are skipped by design: that is what lets a newer
         * recorder's minor additions be read by this build (DS 6.15.2). */
        break;
    }
  }
  return valid;
}

static int32_t replayResolveSessionIndex(SReplaySession *s, const SWlrFileHeader *hdr,
                                         const char *path) {
  if (hdr->processStartWallNs < 0 || hdr->processStartMonoNs < 0) {
    (void)fprintf(stderr, "replay: segment %s has invalid process start timestamps\n", path);
    return -1;
  }
  for (int32_t i = 0; i < s->sessionCount; ++i) {
    if (memcmp(s->sessionIdBySession[i], hdr->sessionId, WLR_SESSION_ID_LEN) != 0) continue;
    if (s->pidBySession[i] != hdr->pid ||
        s->processStartWallNsBySession[i] != hdr->processStartWallNs ||
        s->processStartMonoNsBySession[i] != hdr->processStartMonoNs ||
        strcmp(s->clockDomainIdBySession[i], hdr->clockDomainId) != 0) {
      (void)fprintf(stderr, "replay: segment %s has inconsistent identity fields for its session\n", path);
      return -1;
    }
    if (s->haveSegmentIndexBySession[i] && hdr->segmentIndex <= s->lastSegmentIndexBySession[i]) {
      (void)fprintf(stderr,
                    "replay: segment %s has duplicate or decreasing index %u after %u\n",
                    path, hdr->segmentIndex, s->lastSegmentIndexBySession[i]);
      return -1;
    }
    if (!s->truncatedBySession[i] && s->haveSegmentIndexBySession[i] &&
        hdr->segmentIndex != s->lastSegmentIndexBySession[i] + 1u) {
      (void)fprintf(stderr, "replay: segment %s has index %u after %u; retaining the consecutive prefix\n",
                    path, hdr->segmentIndex, s->lastSegmentIndexBySession[i]);
      s->structurallyComplete = false;
      s->truncatedBySession[i] = true;
    }
    s->lastSegmentIndexBySession[i] = hdr->segmentIndex;
    s->haveSegmentIndexBySession[i] = true;
    return i;
  }
  if (s->sessionCount >= REPLAY_MAX_INPUTS) {
    (void)fprintf(stderr, "replay: more than %d recording sessions supplied\n", REPLAY_MAX_INPUTS);
    return -1;
  }
  int32_t sessionIdx = s->sessionCount++;
  if (hdr->segmentIndex != 0 || hdr->firstEventSeq != 1) {
    (void)fprintf(stderr, "replay: segment %s is missing the start of its session\n", path);
    s->structurallyComplete = false;
    s->truncatedBySession[sessionIdx] = true;
  }
  memcpy(s->sessionIdBySession[sessionIdx], hdr->sessionId, WLR_SESSION_ID_LEN);
  memcpy(s->clockDomainIdBySession[sessionIdx], hdr->clockDomainId, WLR_CLOCK_DOMAIN_ID_LEN);
  s->pidBySession[sessionIdx] = hdr->pid;
  s->processStartWallNsBySession[sessionIdx] = hdr->processStartWallNs;
  s->processStartMonoNsBySession[sessionIdx] = hdr->processStartMonoNs;
  s->lastSegmentIndexBySession[sessionIdx] = hdr->segmentIndex;
  s->haveSegmentIndexBySession[sessionIdx] = true;
  return sessionIdx;
}

static int32_t replayLoadSegment(const char *path, SReplaySession *s, bool *tornTailFound) {
  SReplayBuffer  *buf;
  SWlrFileHeader  hdr;
  size_t          off;
  int32_t         eventStart = s->eventCount;
  bool            oldStructurallyComplete = s->structurallyComplete;
  bool            oldSawSessionStop = s->sawSessionStop;
  bool            oldSawFatal = s->sawFatal;
  bool            oldTornTail = s->tornTail;
  bool            firstFrame = true;
  int64_t         segmentSize = 0;
  if (tornTailFound != NULL) *tornTailFound = false;

  if (s->bufferCount >= REPLAY_MAX_SEGMENTS) {
    /* Named, not a bare -1: the caller reports "cannot read <path>" for every other
     * failure here, which would blame the file for a capacity limit of this tool. */
    (void)fprintf(stderr,
                  "replay: cannot load more than %d segment files in one run (at %s)\n",
                  REPLAY_MAX_SEGMENTS, path);
    return -1;
  }
  if (taosStatFile(path, &segmentSize, NULL, NULL) != 0 || segmentSize <= 0) return -1;
  int64_t totalBytes = segmentSize;
  for (int32_t i = 0; i < s->bufferCount; ++i) {
    if (s->buffers[i].len > INT64_MAX - totalBytes) return -1;
    totalBytes += s->buffers[i].len;
  }
  if (totalBytes > REPLAY_MAX_TOTAL_SEGMENT_BYTES) {
    (void)fprintf(stderr, "replay: total input segments exceed %" PRId64 " bytes\n",
                  (int64_t)REPLAY_MAX_TOTAL_SEGMENT_BYTES);
    return -1;
  }
  buf = &s->buffers[s->bufferCount];
  if (replayReadFile(path, buf) != 0) {
    (void)fprintf(stderr, "replay: cannot read %s\n", path);
    return -1;
  }
  s->bufferCount++;

  if (wlrDecodeFileHeader(buf->buf, (size_t)buf->len, &hdr) != WLR_DECODE_OK) {
    (void)fprintf(stderr, "%s is not a readable recording segment\n", path);
    taosMemoryFreeClear(buf->buf);
    buf->len = 0;
    s->bufferCount--;
    return -1;
  }
  int32_t sessionIdx = replayResolveSessionIndex(s, &hdr, path);
  if (sessionIdx < 0) {
    taosMemoryFreeClear(buf->buf);
    buf->len = 0;
    s->bufferCount--;
    return -1;
  }
  if (s->truncatedBySession[sessionIdx]) return 0;
  uint64_t oldLastEventSeq = s->lastEventSeqBySession[sessionIdx];
  bool oldHaveEventSeq = s->haveEventSeqBySession[sessionIdx];

  off = hdr.headerLength;
  while (off < (size_t)buf->len) {
    SWlrFrameHeader  fh;
    const uint8_t   *payload = NULL;
    uint32_t         consumed = 0;
    EWlrDecodeResult r;

    r = wlrDecodeFrame(buf->buf + off, (size_t)buf->len - off, &fh, &payload, &consumed);
    if (r == WLR_DECODE_MISSING_LF) {
      uint32_t frameLength = wlrGetU32(buf->buf + off);
      size_t frameBytes = (size_t)WLR_FRAME_OFF_TYPE + (size_t)frameLength;
      if (frameBytes < (size_t)buf->len - off) {
        (void)fprintf(stderr, "%s: invalid frame delimiter at offset %zu before later data\n", path, off);
        s->structurallyComplete = false;
        s->truncatedBySession[sessionIdx] = true;
        break;
      }
      s->tornTail = true;
      if (tornTailFound != NULL) *tornTailFound = true;
      break;
    }
    if (r == WLR_DECODE_TORN_TAIL) {
      s->tornTail = true;
      if (tornTailFound != NULL) *tornTailFound = true;
      break;
    }
    if (r != WLR_DECODE_OK) {
      /* Damage in the middle: everything after this point is unusable, and the
       * session is structurally incomplete. */
      (void)fprintf(stderr, "%s: decode error %d at offset %zu\n", path, (int32_t)r, off);
      s->structurallyComplete = false;
      s->truncatedBySession[sessionIdx] = true;
      break;
    }

    if (firstFrame && fh.eventSequence != hdr.firstEventSeq) {
      (void)fprintf(stderr, "%s: first frame sequence does not match segment header\n", path);
      s->structurallyComplete = false;
      s->truncatedBySession[sessionIdx] = true;
      break;
    }
    firstFrame = false;

    if (s->haveEventSeqBySession[sessionIdx] &&
        fh.eventSequence != s->lastEventSeqBySession[sessionIdx] + 1) {
      /* A gap means events were lost, which is exactly what --allow-incomplete
       * exists to gate. */
      s->structurallyComplete = false;
      s->truncatedBySession[sessionIdx] = true;
      break;
    }
    s->lastEventSeqBySession[sessionIdx] = fh.eventSequence;
    s->haveEventSeqBySession[sessionIdx] = true;

    if (fh.type == WLR_FRAME_SESSION_STOP || fh.type == WLR_FRAME_FATAL) {
      int32_t tlvCount = 0;
      EWlrDecodeResult tr = wlrDecodeTlvs(payload, fh.payloadLength, NULL, 0, &tlvCount);
      SWlrTlv *tlvs = NULL;
      if (tr == WLR_DECODE_OK) {
        tlvs = (SWlrTlv *)taosMemoryMalloc((size_t)(tlvCount > 0 ? tlvCount : 1) * sizeof(*tlvs));
        if (tlvs == NULL) {
          tr = WLR_DECODE_NO_SPACE;
        } else {
          tr = wlrDecodeTlvs(payload, fh.payloadLength, tlvs, tlvCount, &tlvCount);
          if (tr == WLR_DECODE_OK) {
            tr = wlrValidateRequiredTags(fh.type, WLR_API_UNKNOWN, 0, tlvs, tlvCount);
          }
        }
      }
      taosMemoryFree(tlvs);
      if (tr == WLR_DECODE_OK) {
        if (fh.type == WLR_FRAME_SESSION_STOP) s->sawSessionStop = true;
        else s->sawFatal = true;
      } else {
        (void)fprintf(stderr, "replay: invalid terminal frame at eventSeq %" PRIu64 " (result %d)\n",
                      fh.eventSequence, (int32_t)tr);
        s->structurallyComplete = false;
        s->truncatedBySession[sessionIdx] = true;
      }
    } else if (fh.type == WLR_FRAME_CHUNK_BEGIN || fh.type == WLR_FRAME_CHUNK_DATA ||
               fh.type == WLR_FRAME_CHUNK_END) {
      (void)fprintf(stderr,
                    "replay: chunked event at eventSeq %" PRIu64
                    " is not supported by this build; marking the session incomplete\n",
                    fh.eventSequence);
      s->structurallyComplete = false;
      s->truncatedBySession[sessionIdx] = true;
    } else if (fh.type == WLR_FRAME_EVENT) {
      /* 256, not 64: a bind frame encodes one TLV group per column, and the format
       * permits minor versions to add optional tags, so a legitimate frame from a newer
       * recorder can carry well over 64. Capacity exhaustion is reported separately as
       * WLR_DECODE_NO_SPACE, and every non-OK result is named below so an incomplete
       * replay cannot look complete. */
      SWlrTlv tlvs[256];
      int32_t tlvCount = 0;
      EWlrDecodeResult tr = wlrDecodeTlvs(payload, fh.payloadLength, tlvs,
                                          (int32_t)(sizeof(tlvs) / sizeof(tlvs[0])), &tlvCount);
      if (tr == WLR_DECODE_NO_SPACE) {
        (void)fprintf(stderr,
                      "replay: event at eventSeq %" PRIu64 " has %d TLVs, exceeding this "
                      "build's decode capacity; skipping it\n",
                      fh.eventSequence, tlvCount);
        s->structurallyComplete = false;
        s->truncatedBySession[sessionIdx] = true;
      } else if (tr != WLR_DECODE_OK) {
        /* Named, not silent: the event is lost either way, but an operator comparing
         * "events in input" against the recording needs to know why. */
        (void)fprintf(stderr,
                      "replay: event at eventSeq %" PRIu64 " could not be decoded (result %d); "
                      "skipping it\n",
                      fh.eventSequence, (int32_t)tr);
        s->structurallyComplete = false;
        s->truncatedBySession[sessionIdx] = true;
      }
      if (tr == WLR_DECODE_OK) {
        if (replayEnsureCap(s) != 0) {
          (void)fprintf(stderr, "too many events to load\n");
          /* Roll back this segment as one unit. Its events borrow buf, so freeing only
           * the buffer would leave dangling pointers in the partial session. */
          s->eventCount = eventStart;
          s->lastEventSeqBySession[sessionIdx] = oldLastEventSeq;
          s->haveEventSeqBySession[sessionIdx] = oldHaveEventSeq;
          s->structurallyComplete = oldStructurallyComplete;
          s->sawSessionStop = oldSawSessionStop;
          s->sawFatal = oldSawFatal;
          s->tornTail = oldTornTail;
          if (tornTailFound != NULL) *tornTailFound = false;
          taosMemoryFreeClear(buf->buf);
          buf->len = 0;
          s->bufferCount--;
          return -1;
        }
        SReplayEvent *ev = &s->events[s->eventCount];
        memset(ev, 0, sizeof(*ev));
        ev->loadOrdinal = s->eventCount;
        ev->eventSeq = fh.eventSequence;
        ev->sessionIdx = sessionIdx;
        bool fieldsOk = replayFillEvent(ev, tlvs, tlvCount);
        bool requiredOk = fieldsOk &&
                          wlrValidateRequiredTags(fh.type, ev->apiType, ev->phase, tlvs, tlvCount) == WLR_DECODE_OK;
        if (!requiredOk) {
          (void)fprintf(stderr, "replay: eventSeq %" PRIu64 " is missing required tags; skipping it\n",
                        ev->eventSeq);
          s->structurallyComplete = false;
          s->truncatedBySession[sessionIdx] = true;
        } else if (!ev->haveMonoNs) {
          ev->monoNs = INT64_MAX;
        } else if (ev->monoNs < hdr.processStartMonoNs) {
          (void)fprintf(stderr,
                        "replay: eventSeq %" PRIu64 " has monotonic time before session start\n",
                        ev->eventSeq);
          s->structurallyComplete = false;
          s->truncatedBySession[sessionIdx] = true;
          ev->monoNs = INT64_MAX;
        }
        if (requiredOk && !s->truncatedBySession[sessionIdx]) s->eventCount++;
      }
    }

    if (s->truncatedBySession[sessionIdx]) break;
    if (consumed == 0) {
      (void)fprintf(stderr, "%s: decoder consumed zero bytes at offset %zu\n", path, off);
      s->structurallyComplete = false;
      s->truncatedBySession[sessionIdx] = true;
      break;
    }
    off += consumed;
  }

  return 0;
}

static int32_t replayCmpPaths(const void *a, const void *b) {
  const char *const *x = (const char *const *)a;
  const char *const *y = (const char *const *)b;
  const char *xDash = strrchr(*x, '-');
  const char *yDash = strrchr(*y, '-');
  uint64_t xIndex = 0, yIndex = 0;
  bool xValid = false, yValid = false;
  if (xDash != NULL) {
    const unsigned char *p = (const unsigned char *)(xDash + 1);
    xValid = *p >= '0' && *p <= '9';
    for (; *p >= '0' && *p <= '9'; ++p) {
      if (xIndex > (UINT64_MAX - (uint64_t)(*p - '0')) / 10) { xValid = false; break; }
      xIndex = xIndex * 10 + (uint64_t)(*p - '0');
    }
    if (xValid && strcmp((const char *)p, ".wlr") != 0) xValid = false;
  }
  if (yDash != NULL) {
    const unsigned char *p = (const unsigned char *)(yDash + 1);
    yValid = *p >= '0' && *p <= '9';
    for (; *p >= '0' && *p <= '9'; ++p) {
      if (yIndex > (UINT64_MAX - (uint64_t)(*p - '0')) / 10) { yValid = false; break; }
      yIndex = yIndex * 10 + (uint64_t)(*p - '0');
    }
    if (yValid && strcmp((const char *)p, ".wlr") != 0) yValid = false;
  }
  if (!xValid || !yValid) return strcmp(*x, *y);
  if (xIndex < yIndex) return -1;
  if (xIndex > yIndex) return 1;
  return strcmp(*x, *y);
}

typedef struct {
  const char     *path;
  SWlrFileHeader header;
  bool           haveHeader;
} SReplayOrderedInput;

static bool replayReadHeaderPrefix(const char *path, SWlrFileHeader *header) {
  uint8_t   prefix[WLR_FILE_HEADER_V0_SIZE];
  TdFilePtr file = taosOpenFile(path, TD_FILE_READ);
  if (file == NULL) return false;
  int64_t total = 0;
  while (total < (int64_t)sizeof(prefix)) {
    int64_t count = taosReadFile(file, prefix + total, (int64_t)sizeof(prefix) - total);
    if (count <= 0) break;
    total += count;
  }
  uint16_t headerLength = total == sizeof(prefix) ? wlrGetU16(prefix + WLR_HDR_OFF_HDRLEN) : 0;
  uint8_t *bytes = headerLength >= sizeof(prefix) ? (uint8_t *)taosMemoryMalloc(headerLength) : NULL;
  if (bytes != NULL) {
    memcpy(bytes, prefix, sizeof(prefix));
    while (total < headerLength) {
      int64_t count = taosReadFile(file, bytes + total, headerLength - total);
      if (count <= 0) break;
      total += count;
    }
  }
  (void)taosCloseFile(&file);
  bool valid = bytes != NULL && total == headerLength &&
               wlrDecodeFileHeader(bytes, headerLength, header) == WLR_DECODE_OK;
  taosMemoryFree(bytes);
  return valid;
}

static int32_t replayCmpOrderedInputs(const void *a, const void *b) {
  const SReplayOrderedInput *x = a;
  const SReplayOrderedInput *y = b;
  if (x->haveHeader != y->haveHeader) return x->haveHeader ? -1 : 1;
  if (x->haveHeader) {
    int32_t identityOrder = memcmp(x->header.sessionId, y->header.sessionId, WLR_SESSION_ID_LEN);
    if (identityOrder != 0) return identityOrder;
    if (x->header.pid < y->header.pid) return -1;
    if (x->header.pid > y->header.pid) return 1;
    if (x->header.processStartWallNs < y->header.processStartWallNs) return -1;
    if (x->header.processStartWallNs > y->header.processStartWallNs) return 1;
    if (x->header.processStartMonoNs < y->header.processStartMonoNs) return -1;
    if (x->header.processStartMonoNs > y->header.processStartMonoNs) return 1;
    if (x->header.segmentIndex < y->header.segmentIndex) return -1;
    if (x->header.segmentIndex > y->header.segmentIndex) return 1;
  }
  return replayCmpPaths(&x->path, &y->path);
}

static void replayFreeOrderedInputs(SReplayOrderedInput *inputs, int32_t count) {
  for (int32_t i = 0; i < count; ++i) taosMemoryFree((void *)inputs[i].path);
  taosMemoryFree(inputs);
}

static int32_t replayCollectPath(const char *path, SReplayOrderedInput *inputs, int32_t *count) {
  if (!taosIsDir(path)) {
    if (*count >= REPLAY_MAX_SEGMENTS) return -1;
    size_t length = strlen(path);
    char *copy = (char *)taosMemoryMalloc(length + 1);
    if (copy == NULL) return -1;
    memcpy(copy, path, length + 1);
    inputs[*count].path = copy;
    inputs[*count].haveHeader = replayReadHeaderPrefix(copy, &inputs[*count].header);
    ++(*count);
    return 0;
  }

  TdDirPtr directory = taosOpenDir(path);
  if (directory == NULL) return -1;
  int32_t found = 0;
  int32_t code = 0;
  TdDirEntryPtr entry;
  while ((entry = taosReadDir(directory)) != NULL) {
    const char *name = taosGetDirEntryName(entry);
    if (name == NULL) continue;
    size_t nameLength = strlen(name);
    if (nameLength <= 4 || strcmp(name + nameLength - 4, ".wlr") != 0) continue;
    size_t pathLength = strlen(path);
    size_t separatorLength = strlen(TD_DIRSEP);
    if (*count >= REPLAY_MAX_SEGMENTS || nameLength > SIZE_MAX - separatorLength - 1 ||
        pathLength > SIZE_MAX - separatorLength - nameLength - 1) {
      code = -1;
      break;
    }
    size_t fullLength = pathLength + separatorLength + nameLength;
    char *fullPath = (char *)taosMemoryMalloc(fullLength + 1);
    if (fullPath == NULL) {
      code = -1;
      break;
    }
    (void)snprintf(fullPath, fullLength + 1, "%s%s%s", path, TD_DIRSEP, name);
    if (taosIsDir(fullPath)) {
      taosMemoryFree(fullPath);
      continue;
    }
    inputs[*count].path = fullPath;
    inputs[*count].haveHeader = replayReadHeaderPrefix(fullPath, &inputs[*count].header);
    ++(*count);
    ++found;
  }
  (void)taosCloseDir(&directory);
  if (found == 0) (void)fprintf(stderr, "no .wlr segments found in %s\n", path);
  if (code != 0) (void)fprintf(stderr, "replay: segment collection failed in %s\n", path);
  return code == 0 && found > 0 ? 0 : -1;
}

static int32_t replayNormalizeTimeline(SReplaySession *session) {
  for (int32_t i = 1; i < session->sessionCount; ++i) {
    if (session->clockDomainIdBySession[0][0] == 0 ||
        session->clockDomainIdBySession[i][0] == 0 ||
        strcmp(session->clockDomainIdBySession[0], session->clockDomainIdBySession[i]) != 0) {
      (void)fprintf(stderr,
                    "replay: recording sessions have missing or different monotonic clock-domain identities\n");
      return -1;
    }
  }
  int64_t origin = INT64_MAX;
  for (int32_t i = 0; i < session->eventCount; ++i) {
    if (session->events[i].haveMonoNs && session->events[i].monoNs < origin) {
      origin = session->events[i].monoNs;
    }
  }
  if (origin == INT64_MAX) return 0;
  for (int32_t i = 0; i < session->eventCount; ++i) {
    if (session->events[i].haveMonoNs) session->events[i].monoNs -= origin;
  }
  return 0;
}

static int32_t replayCmpEvents(const void *a, const void *b) {
  const SReplayEvent *x = (const SReplayEvent *)a;
  const SReplayEvent *y = (const SReplayEvent *)b;
  /* Monotonic time first, event sequence as the tiebreak. eventSeq alone would
   * serialise independent connections that ran concurrently; time alone has ties
   * (DS 6.19). */
  if (x->monoNs != y->monoNs) return (x->monoNs < y->monoNs) ? -1 : 1;
  if (x->eventSeq != y->eventSeq) return (x->eventSeq < y->eventSeq) ? -1 : 1;
  /* Same time and same sequence can only happen across sessions; order by input so
   * the merge is deterministic. */
  if (x->sessionIdx != y->sessionIdx) return (x->sessionIdx < y->sessionIdx) ? -1 : 1;
  if (x->loadOrdinal != y->loadOrdinal) return (x->loadOrdinal < y->loadOrdinal) ? -1 : 1;
  return 0;
}

/*
 * Count BEGINs with no matching RETURN/DONE -- the signature of a hung call.
 *
 * Two linear passes over an open-addressed table, not the nested scan this used to be.
 * The pairing key is (sessionIdx, callId), and the old form rescanned every event for
 * each BEGIN: quadratic, so at the REPLAY_MAX_EVENTS ceiling of 250000 events it was on
 * the order of 1e12 comparisons before replay even started. Since this runs
 * unconditionally at load, a large recording appeared to hang in startup.
 *
 * The table is power-of-two sized at >= 2x the event count so probing stays short, and
 * is allocated once. If it cannot be allocated the count is reported as 0 rather than
 * falling back to the quadratic walk -- the number is a diagnostic, and stalling the
 * tool to produce it is the worse trade.
 */
static int32_t replayCountUnfinished(SReplaySession *s) {
  int32_t i;
  s->unfinishedCalls = 0;
  s->unfinishedCountKnown = false;
  if (s->eventCount <= 0) {
    s->unfinishedCountKnown = true;
    return 0;
  }

  size_t cap = 1;
  while (cap < (size_t)s->eventCount * 2) {
    if (cap > SIZE_MAX / 2) {
      (void)fprintf(stderr, "replay: unfinished-call table size overflow; count is unavailable\n");
      return 0;
    }
    cap <<= 1;
  }

  /* `used` distinguishes an empty slot from a stored callId of any value. */
  typedef struct {
    uint64_t key;
    int32_t  sessionIdx;
    uint16_t apiType;
    bool     stmt2Async;
    bool     completed;
    bool     used;
  } SEndedSlot;

  SEndedSlot *ended = (SEndedSlot *)taosMemoryCalloc(cap, sizeof(SEndedSlot));
  if (ended == NULL) {
    (void)fprintf(stderr, "replay: cannot allocate unfinished-call table; count is unavailable\n");
    return 0;
  }

  /* Pass 1: index BEGIN events so terminal handling uses the originating API. */
  for (i = 0; i < s->eventCount; ++i) {
    if (s->events[i].phase != WLR_PHASE_BEGIN) continue;
    uint64_t k = s->events[i].callId;
    if (k == 0) continue;
    size_t   h = ((size_t)(k * REPLAY_BEGIN_HASH_MULTIPLIER) ^ (size_t)s->events[i].sessionIdx) & (cap - 1);
    size_t probe = 0;
    bool duplicate = false;
    while (ended[h].used && probe < cap) {
      if (ended[h].key == k && ended[h].sessionIdx == s->events[i].sessionIdx) {
        duplicate = true;
        break;
      }
      h = (h + 1) & (cap - 1);
      ++probe;
    }
    if (probe == cap) {
      taosMemoryFree(ended);
      (void)fprintf(stderr, "replay: unfinished-call table exhausted; count is unavailable\n");
      return 0;
    }
    if (duplicate) {
      (void)fprintf(stderr, "replay: duplicate BEGIN callId=%" PRIu64 " session=%d\n", k,
                    s->events[i].sessionIdx);
      taosMemoryFree(ended);
      return -1;
    }
    ended[h].used = true;
    ended[h].key = k;
    ended[h].sessionIdx = s->events[i].sessionIdx;
    ended[h].apiType = s->events[i].apiType;
    ended[h].stmt2Async = s->events[i].stmt2Async;
  }

  /* Pass 2: asynchronous query/fetch RETURN is dispatch-only; DONE is terminal. */
  for (i = 0; i < s->eventCount; ++i) {
    if (s->events[i].phase == WLR_PHASE_BEGIN) continue;
    uint64_t k = s->events[i].callId;
    if (k == 0) continue;
    size_t   h = ((size_t)(k * REPLAY_BEGIN_HASH_MULTIPLIER) ^ (size_t)s->events[i].sessionIdx) & (cap - 1);
    size_t probe = 0;
    while (ended[h].used && probe < cap) {
      if (ended[h].key == k && ended[h].sessionIdx == s->events[i].sessionIdx) {
        if (replayPhaseTerminatesCall(ended[h].apiType, ended[h].stmt2Async,
                                      s->events[i].phase)) ended[h].completed = true;
        break;
      }
      h = (h + 1) & (cap - 1);
      ++probe;
    }
  }

  /* Pass 3: count indexed BEGIN calls without the API-specific terminal phase. */
  for (i = 0; i < (int32_t)cap; ++i) {
    if (ended[i].used && !ended[i].completed) s->unfinishedCalls++;
  }

  s->unfinishedCountKnown = true;
  taosMemoryFree(ended);
  return 0;
}

int32_t replayLoadInputs(const SReplayOpts *opts, SReplaySession *session) {
  int32_t i;
  int32_t segmentCount = 0;
  SReplayOrderedInput *orderedInputs =
      (SReplayOrderedInput *)taosMemoryCalloc(REPLAY_MAX_SEGMENTS, sizeof(*orderedInputs));
  if (orderedInputs == NULL) return REPLAY_EXIT_INTERNAL;

  session->structurallyComplete = true;

  for (i = 0; i < opts->inputCount; ++i) {
    const char *p = opts->inputs[i];
    /*
     * A guardrail against a mistyped path, NOT a security boundary.
     *
     * These paths come from the operator's own argv (replayParseArgs), never from the
     * recording bytes, so there is no untrusted input steering which file is opened and
     * nothing here to escalate. The check stays because reading a segment from outside
     * the intended directory is more often a mistake than an intent -- but it is
     * trivially bypassable (a symlink, or an absolute path with no ".."), and it also
     * rejects legitimate relative paths, so it must not be mistaken for containment.
     * Anyone running this tool can already read whatever their own uid can.
     */
    if (replayHasParentComponent(p)) {
      (void)fprintf(stderr, "input path must not contain '..': %s\n", p);
      replayFreeOrderedInputs(orderedInputs, segmentCount);
      return REPLAY_EXIT_INPUT;
    }
    if (replayCollectPath(p, orderedInputs, &segmentCount) != 0) {
      replayFreeOrderedInputs(orderedInputs, segmentCount);
      return REPLAY_EXIT_INPUT;
    }
  }
  taosSort(orderedInputs, (size_t)segmentCount, sizeof(*orderedInputs), replayCmpOrderedInputs);
  for (i = 0; i < segmentCount; ++i) {
    bool tornTailFound = false;
    if (replayLoadSegment(orderedInputs[i].path, session, &tornTailFound) != 0) {
      replayFreeOrderedInputs(orderedInputs, segmentCount);
      return REPLAY_EXIT_INPUT;
    }
    if (tornTailFound && i + 1 < segmentCount && orderedInputs[i].haveHeader) {
      for (int32_t next = i + 1; next < segmentCount; ++next) {
        if (!orderedInputs[next].haveHeader) continue;
        const SWlrFileHeader *current = &orderedInputs[i].header;
        const SWlrFileHeader *later = &orderedInputs[next].header;
        if (memcmp(current->sessionId, later->sessionId, WLR_SESSION_ID_LEN) != 0 ||
            current->pid != later->pid ||
            current->processStartWallNs != later->processStartWallNs ||
            current->processStartMonoNs != later->processStartMonoNs) continue;
        (void)fprintf(stderr, "replay: non-final segment %s has a torn tail; the session is incomplete\n",
                      orderedInputs[i].path);
        session->structurallyComplete = false;
        for (int32_t sessionIdx = 0; sessionIdx < session->sessionCount; ++sessionIdx) {
          if (memcmp(session->sessionIdBySession[sessionIdx], current->sessionId,
                     WLR_SESSION_ID_LEN) == 0) {
            session->truncatedBySession[sessionIdx] = true;
            break;
          }
        }
        break;
      }
    }
  }
  replayFreeOrderedInputs(orderedInputs, segmentCount);

  if (session->eventCount == 0) {
    (void)fprintf(stderr, "no events found in the given input\n");
    return REPLAY_EXIT_INPUT;
  }

  if (replayNormalizeTimeline(session) != 0) return REPLAY_EXIT_INPUT;

  /* k-way merge across inputs by (mono time, eventSeq) -- DS 6.17 step 7. */
  taosSort(session->events, (size_t)session->eventCount, sizeof(SReplayEvent), replayCmpEvents);
  if (replayCountUnfinished(session) != 0) return REPLAY_EXIT_INPUT;

  /*
   * captureComplete is false when fail-open happened or a gap was seen. Unfinished
   * calls are deliberately NOT part of this test: a BEGIN with no END is how a hung
   * call is recorded, and refusing to replay it would block the feature's main use
   * case (DS 6.15.1, 6.17).
   */
  bool captureComplete = !session->sawFatal && session->structurallyComplete;
  if (opts->mode == REPLAY_MODE_REPLAY && !captureComplete && !opts->allowIncomplete) {
    (void)fprintf(stderr,
                  "input is incomplete (%s%s); refusing to replay.\n"
                  "Pass --allow-incomplete to replay the verifiable prefix.\n",
                  session->sawFatal ? "recorder failed open" : "",
                  session->structurallyComplete ? "" : " segment/event gap or frame damage");
    return REPLAY_EXIT_INPUT;
  }

  return REPLAY_EXIT_OK;
}

void replayFreeSession(SReplaySession *session) {
  int32_t i;
  for (i = 0; i < session->bufferCount; ++i) {
    taosMemoryFreeClear(session->buffers[i].buf);
  }
  taosMemoryFreeClear(session->events);
  memset(session, 0, sizeof(*session));
}

static const char *replayApiName(uint16_t api) {
  switch (api) {
    case WLR_API_CONNECT: return "connect";
    case WLR_API_CONNECT_WITH: return "connect_with";
    case WLR_API_CONNECT_AUTH: return "connect_auth";
    case WLR_API_CONNECT_TOTP: return "connect_totp";
    case WLR_API_CONNECT_TOKEN: return "connect_token";
    case WLR_API_CLOSE: return "close";
    case WLR_API_OPTIONS_CONNECTION: return "options_connection";
    case WLR_API_SELECT_DB: return "select_db";
    case WLR_API_RESET_CURRENT_DB: return "reset_current_db";
    case WLR_API_TXN_BEGIN: return "txn_begin";
    case WLR_API_TXN_COMMIT: return "txn_commit";
    case WLR_API_TXN_ROLLBACK: return "txn_rollback";
    case WLR_API_KILL_QUERY: return "kill_query";
    case WLR_API_STOP_QUERY: return "stop_query";
    case WLR_API_QUERY: return "query";
    case WLR_API_QUERY_WITH_REQID: return "query_with_reqid";
    case WLR_API_QUERY_A: return "query_a";
    case WLR_API_QUERY_A_WITH_REQID: return "query_a_with_reqid";
    case WLR_API_VALIDATE_SQL: return "validate_sql";
    case WLR_API_FETCH_ROW: return "fetch_row";
    case WLR_API_FETCH_BLOCK: return "fetch_block";
    case WLR_API_FETCH_BLOCK_S: return "fetch_block_s";
    case WLR_API_FETCH_RAW_BLOCK: return "fetch_raw_block";
    case WLR_API_FETCH_ROWS_A: return "fetch_rows_a";
    case WLR_API_FETCH_RAW_BLOCK_A: return "fetch_raw_block_a";
    case WLR_API_FREE_RESULT: return "free_result";
    case WLR_API_STMT_INIT: return "stmt_init";
    case WLR_API_STMT_INIT_WITH_REQID: return "stmt_init_with_reqid";
    case WLR_API_STMT_INIT_WITH_OPTIONS: return "stmt_init_with_options";
    case WLR_API_STMT_PREPARE: return "stmt_prepare";
    case WLR_API_STMT_SET_TBNAME_TAGS: return "stmt_set_tbname_tags";
    case WLR_API_STMT_SET_TBNAME: return "stmt_set_tbname";
    case WLR_API_STMT_SET_TAGS: return "stmt_set_tags";
    case WLR_API_STMT_SET_SUB_TBNAME: return "stmt_set_sub_tbname";
    case WLR_API_STMT_BIND_PARAM: return "stmt_bind_param";
    case WLR_API_STMT_BIND_PARAM_BATCH: return "stmt_bind_param_batch";
    case WLR_API_STMT_BIND_SINGLE_PARAM_BATCH: return "stmt_bind_single_param_batch";
    case WLR_API_STMT_ADD_BATCH: return "stmt_add_batch";
    case WLR_API_STMT_EXECUTE: return "stmt_execute";
    case WLR_API_STMT_USE_RESULT: return "stmt_use_result";
    case WLR_API_STMT_CLOSE: return "stmt_close";
    case WLR_API_STMT2_INIT: return "stmt2_init";
    case WLR_API_STMT2_PREPARE: return "stmt2_prepare";
    case WLR_API_STMT2_BIND_PARAM: return "stmt2_bind_param";
    case WLR_API_STMT2_BIND_PARAM_A: return "stmt2_bind_param_a";
    case WLR_API_STMT2_BIND_PARAM_COLUMN: return "stmt2_bind_param_column";
    case WLR_API_STMT2_BIND_PARAM_COLUMN_A: return "stmt2_bind_param_column_a";
    case WLR_API_STMT2_EXEC: return "stmt2_exec";
    case WLR_API_STMT2_RESULT: return "stmt2_result";
    case WLR_API_STMT2_CLOSE: return "stmt2_close";
    case WLR_API_SML_INSERT: return "schemaless_insert";
    case WLR_API_SML_INSERT_WITH_REQID: return "schemaless_insert_with_reqid";
    case WLR_API_SML_INSERT_RAW: return "schemaless_insert_raw";
    case WLR_API_SML_INSERT_RAW_WITH_REQID: return "schemaless_insert_raw_with_reqid";
    case WLR_API_SML_INSERT_TTL: return "schemaless_insert_ttl";
    case WLR_API_SML_INSERT_TTL_WITH_REQID: return "schemaless_insert_ttl_with_reqid";
    case WLR_API_SML_INSERT_RAW_TTL: return "schemaless_insert_raw_ttl";
    case WLR_API_SML_INSERT_RAW_TTL_WITH_REQID: return "schemaless_insert_raw_ttl_with_reqid";
    case WLR_API_SML_INSERT_RAW_TTL_WITH_REQID_TBNAME_KEY:
      return "schemaless_insert_raw_ttl_with_reqid_tbname_key";
    case WLR_API_SML_INSERT_TTL_WITH_REQID_TBNAME_KEY:
      return "schemaless_insert_ttl_with_reqid_tbname_key";
    default: return "other";
  }
}

void replayPrintInspect(const SReplayOpts *opts, const SReplaySession *session) {
  int32_t i;

  (void)printf("segments loaded      : %d\n", session->bufferCount);
  (void)printf("events               : %d\n", session->eventCount);
  (void)printf("session stop marker  : %s\n", session->sawSessionStop ? "yes" : "no");
  (void)printf("fatal marker         : %s\n", session->sawFatal ? "yes" : "no");
  (void)printf("trailing partial frame: %s\n", session->tornTail ? "yes (normal for an active copy)" : "no");
  (void)printf("structurally complete: %s\n", session->structurallyComplete ? "yes" : "no");
  if (session->unfinishedCountKnown) {
    (void)printf("unfinished calls     : %d%s\n", session->unfinishedCalls,
                 session->unfinishedCalls > 0 ? "  (not a defect: a hung call looks like this)" : "");
  } else {
    (void)printf("unfinished calls     : unavailable\n");
  }

  if (session->eventCount > 0) {
    int32_t first = 0;
    int32_t last = session->eventCount - 1;
    while (first <= last && session->events[first].monoNs == INT64_MAX) ++first;
    while (last >= first && session->events[last].monoNs == INT64_MAX) --last;
    if (first <= last) {
      int64_t span = session->events[last].monoNs - session->events[first].monoNs;
      (void)printf("recorded span        : %.3f s\n", (double)span / 1e9);
    }
  }

  /*
   * SQL is shown only with --verbose. The default output stays safe to paste into a
   * ticket: recordings contain unmasked statements and bind values (FS 6, DS 6.24).
   */
  if (!opts->verbose) {
    (void)printf("\n(use --verbose to list events, including recorded SQL)\n");
    return;
  }

  const int32_t apiNameWidth = 50;
  (void)printf("\n%-8s %-*s %-6s %-8s %-8s %s\n", "seq", apiNameWidth, "api", "phase", "conn", "call",
               "sql");
  for (i = 0; i < session->eventCount; ++i) {
    const SReplayEvent *e = &session->events[i];
    int32_t sqlLen = e->sqlLen > (uint32_t)INT32_MAX ? INT32_MAX : (int32_t)e->sqlLen;
    (void)printf("%-8" PRIu64 " %-*s %-6u %-8" PRIu64 " %-8" PRIu64 " %.*s\n", e->eventSeq,
                 apiNameWidth, replayApiName(e->apiType), (unsigned)e->phase, e->connId, e->callId, sqlLen,
                 e->sql != NULL ? e->sql : "");
  }
}
