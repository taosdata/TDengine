/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

/*
 * Decode a recorder-produced segment file and assert it is well formed.
 *
 * This is the piece that closes the loop without taosreplay: wlrFormatTest proves
 * the codec is self-consistent, and this proves the RECORDER's output is readable
 * by that codec. A round-trip through the encoder alone cannot catch a writer that
 * mis-sizes a frame, skips a sequence number, or emits a payload the validate
 * table rejects.
 *
 * Usage:
 *   wlrSegmentReadTest <path-to-segment.wlr>
 *
 * Synthesized-fixture tests always run. Passing a recorder-produced segment path
 * additionally validates real output; only that test skips when no path is supplied.
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <new>
#include <string>
#include <unordered_map>
#include <vector>

extern "C" {
#include "wlrFormat.h"
#include "os.h"
#include "tcrc32c.h"
}

namespace {

bool requirePositive(int32_t value, const char* what) {
  if (value > 0) return true;
  ADD_FAILURE() << what << " must be positive";
  return false;
}

bool advanceOffset(int32_t* offset, int32_t encoded, size_t capacity) {
  if (*offset < 0 || encoded <= 0 || (size_t)*offset > capacity ||
      (size_t)encoded > capacity - (size_t)*offset) {
    ADD_FAILURE() << "TLV encode overflowed the fixture buffer at offset " << *offset;
    return false;
  }
  *offset += encoded;
  return true;
}

const char *gSegmentPath = nullptr;

enum class ReadFileResult { kOk, kUnreadable, kTooLarge };

ReadFileResult readFile(const char *path, std::vector<uint8_t> *out) {
  /* The command-line validator supports regular segment files only. The project
   * OS layer deliberately forbids direct stat/lstat calls; reject directories here,
   * while the positive-size check below rejects FIFOs and zero-sized devices. */
  if (taosIsDir(path)) return ReadFileResult::kUnreadable;
  int64_t size = 0;
  if (taosStatFile(path, &size, nullptr, nullptr) != 0 || size <= 0) {
    return ReadFileResult::kUnreadable;
  }
  /* Keep this whole-file test helper bounded against hostile paths. The production
   * reader may stream a larger chunked logical event; this validator deliberately
   * accepts only one maximum frame beyond the normal rotation target. */
  constexpr int64_t kMaxReadableSegmentBytes =
      (int64_t)WLR_SEGMENT_ROTATE_SIZE + (int64_t)WLR_MAX_FRAME_PAYLOAD;
  if (size > kMaxReadableSegmentBytes) {
    return ReadFileResult::kTooLarge;
  }

  /* Allocated BEFORE the file is opened. resize() sizes from an untrusted stat value on
   * a path taken from argv[1], and a throw here with the descriptor already open would
   * leak it -- ordering the allocation first makes that impossible without needing an
   * RAII wrapper, and avoids opening a file that cannot be buffered anyway. */
  try {
    out->resize(static_cast<size_t>(size));
  } catch (const std::bad_alloc &) {
    return ReadFileResult::kUnreadable;
  } catch (const std::length_error &) {
    return ReadFileResult::kUnreadable;
  }

  TdFilePtr f = taosOpenFile(path, TD_FILE_READ);
  if (f == nullptr) {
    out->clear();
    return ReadFileResult::kUnreadable;
  }

  int64_t got = 0;
  while (got < size) {
    int64_t n = taosReadFile(f, out->data() + got, size - got);
    if (n <= 0) break;
    got += n;
  }
  uint8_t extra = 0;
  int64_t trailing = (got == size) ? taosReadFile(f, &extra, 1) : 0;
  (void)taosCloseFile(&f);

  /* A short read (I/O error, or the file shrank between stat and read), or an
   * extra byte (the file grew after stat), must not leave a partly-populated buffer
   * behind: the tail would be zeroed padding that validateSegmentBuffer could mistake
   * for real frame bytes. Clear it so failure is unambiguous if a caller reuses it. */
  if (got != size || trailing != 0) {
    GTEST_LOG_(ERROR) << "recording changed or became unreadable while reading " << path
                      << ": expected=" << size << " read=" << got
                      << " trailingRead=" << trailing;
    out->clear();
    return ReadFileResult::kUnreadable;
  }
  return ReadFileResult::kOk;
}

/*
 * Read a u64-valued TLV defensively. wlrGetU64 always reads 8 bytes and
 * wlrDecodeTlvs reports value==NULL for a zero-length TLV while still returning
 * OK, so a short or empty numeric tag in an untrusted recording would otherwise
 * be a null dereference or an out-of-bounds read. A malformed tag is reported here;
 * the caller uses false only to stop trusting derived metadata, without another
 * assertion for the same damage.
 */
bool tlvU64(const SWlrTlv &tlv, uint64_t *out) {
  if (tlv.value == NULL || tlv.length != sizeof(uint64_t)) {
    ADD_FAILURE() << "tag " << WLR_TAG_BASE(tlv.tag) << " carries " << tlv.length
                  << " bytes, expected 8";
    *out = 0;
    return false;
  }
  *out = wlrGetU64(tlv.value);
  return true;
}

/* Only what the ordering and required-tag checks below actually consume. CONN_ID,
 * RESULT_ID and SQL are validated by the required-tag table rather than here, so
 * carrying them would be dead weight. */
struct EventSummary {
  uint64_t eventSeq;
  /* Retained because required-tag validation and its diagnostics use it below. */
  uint16_t apiType;
  uint16_t phase;
  uint64_t callId;
};

}  // namespace

static bool credentialValueContains(const SWlrTlv &tlv, const char *sentinel) {
  const size_t sentinelLen = sentinel != nullptr ? std::strlen(sentinel) : 0;
  if (sentinel == nullptr || sentinelLen == 0 || tlv.value == nullptr || tlv.length < sentinelLen) {
    return false;
  }
  const char *hay = reinterpret_cast<const char *>(tlv.value);
  const char *end = hay + tlv.length;
  return std::search(hay, end, sentinel, sentinel + sentinelLen) != end;
}

static void validateSegmentBuffer(const std::vector<uint8_t> &buf,
                                  const char *credentialLeakSentinel) {
  ASSERT_NE(credentialLeakSentinel, nullptr);
  ASSERT_GT(std::strlen(credentialLeakSentinel), 0u) << "credential sentinel must be configured";

  SWlrFileHeader hdr;
  ASSERT_EQ(wlrDecodeFileHeader(buf.data(), buf.size(), &hdr), WLR_DECODE_OK)
      << "recorder wrote a header the parser rejects";
  EXPECT_EQ(hdr.major, WLR_FORMAT_MAJOR);
  EXPECT_GE(hdr.headerLength, WLR_FILE_HEADER_SIZE);
  EXPECT_NE(hdr.pid, 0u);

  // Walk every frame. A recording is only useful if the whole prefix decodes.
  size_t                    off = hdr.headerLength;
  uint64_t                  prevSeq = 0;
  bool                      firstFrame = true;
  int32_t                   frames = 0;
  std::vector<EventSummary> events;
  EWlrDecodeResult          tail = WLR_DECODE_OK;
  constexpr int32_t         kInlineTlvs = 256;
  SWlrTlv                   inlineTlvs[kInlineTlvs];
  std::vector<SWlrTlv>      heapTlvs;

  while (off < buf.size()) {
    SWlrFrameHeader fh;
    const uint8_t  *payload = nullptr;
    uint32_t        consumed = 0;

    EWlrDecodeResult r = wlrDecodeFrame(buf.data() + off, buf.size() - off, &fh, &payload, &consumed);
    if (r != WLR_DECODE_OK) {
      tail = r;
      break;
    }

    // Sequence numbers must be contiguous within this segment. A gap means the writer
    // published out of order or lost a slot, which would desynchronise replay. This
    // validator accepts one segment path, so cross-segment continuity is checked when
    // replay input merges all segments in a session.
    if (firstFrame) {
      EXPECT_EQ(fh.eventSequence, hdr.firstEventSeq)
          << "first frame sequence does not match header firstEventSeq";
      firstFrame = false;
    } else {
      EXPECT_EQ(fh.eventSequence, prevSeq + 1)
          << "eventSeq gap after " << prevSeq << " in frame " << frames;
    }
    prevSeq = fh.eventSequence;

    /* Sized from this frame's actual payload rather than a fixed bound. A TLV is
     * at least 8 bytes on the wire (2-byte tag + 2-byte flags + 4-byte length), so
     * payloadLength / 8 + 1 can never be too small. A fixed 64-element array
     * rejected legitimate bind frames that encode many columns as "undecodable".
     *
     * The common case stays on the stack: almost every frame carries well under
     * kInlineTlvs TLVs, and a max-size 1 MiB payload would otherwise allocate
     * ~131k elements (~2 MB) per frame, churning the heap when validating a large
     * recording. Both scratch buffers live across iterations, so validating many
     * frames neither zeroes 4 KiB nor constructs/destroys a vector per frame. */
    SWlrTlv *tlvs = inlineTlvs;
    int32_t   tlvCap = kInlineTlvs;
    int32_t   tlvCount = 0;

    const int64_t needed = (int64_t)(fh.payloadLength / WLR_TLV_HEADER_SIZE) + 1;
    if (needed > kInlineTlvs) {
      try {
        heapTlvs.resize(static_cast<size_t>(needed));
      } catch (const std::bad_alloc &) {
        ADD_FAILURE() << "unable to allocate TLV scratch buffer";
        tail = WLR_DECODE_BAD_LENGTH;
        break;
      } catch (const std::length_error &) {
        ADD_FAILURE() << "TLV scratch buffer size is invalid";
        tail = WLR_DECODE_BAD_LENGTH;
        break;
      }
      tlvs = heapTlvs.data();
      tlvCap = static_cast<int32_t>(needed);
    }
    if (wlrDecodeTlvs(payload, fh.payloadLength, tlvs, tlvCap, &tlvCount) != WLR_DECODE_OK) {
      ADD_FAILURE() << "frame " << frames << " has an undecodable payload";
      tail = WLR_DECODE_BAD_LENGTH;
      break;
    }

    EventSummary ev = {};
    ev.eventSeq = fh.eventSequence;
    bool sawWall = false, sawMono = false, sawTid = false;
    bool eventMetadataValid = true;

    /* No tag may carry a password. Checked per-TLV rather than by searching the
     * whole file: the format deliberately stores SQL and usernames in plaintext,
     * and either can legitimately contain the literal (`select * from taosdata`,
     * a user named taosdata_admin), so a raw blob scan reports those as leaks.
     * Only the endpoint, database, and connection-option text tags are inspected,
     * never the tag/length framing. USER and SQL are deliberately excluded because
     * both are expected plaintext and may legitimately equal or contain the sentinel.
     *
     * This is a weak heuristic: it only detects the default deployment password,
     * not custom ones. But the recorder already guarantees it never reads
     * credentials (DS 6.6, FS 6), so this is defense-in-depth rather than the
     * primary protection — it catches an accidental capture of a default-password
     * endpoint or a mistaken tag addition. DB/endpoint strings containing the
     * substring are false positives the heuristic accepts. */
    for (int32_t i = 0; i < tlvCount; ++i) {
      uint16_t base = WLR_TAG_BASE(tlvs[i].tag);
      bool scanText = base == WLR_TAG_ENDPOINT || base == WLR_TAG_DB ||
                      base == WLR_TAG_CONN_OPT_NAME || base == WLR_TAG_CONN_OPT_VALUE;
      if (!scanText) continue;
      if (tlvs[i].value == NULL || tlvs[i].length == 0) continue;

      /* Searched in place. std::string would copy every inspected value -- these
       * are borrowed pointers into the already-loaded segment buffer, and a
       * per-TLV heap allocation is pure overhead when validating a large file.
       * std::string_view would be the idiomatic answer, but this file must build
       * under the GCC 7 core image, whose default dialect predates it. */
      bool leaked = credentialValueContains(tlvs[i], credentialLeakSentinel);
      EXPECT_FALSE(leaked)
          << "frame " << frames << " tag " << base << " carries the credential sentinel";
    }

    for (int32_t i = 0; i < tlvCount; ++i) {
      uint16_t tag = WLR_TAG_BASE(tlvs[i].tag);
      uint64_t v = 0;
      switch (tag) {
        case WLR_TAG_WALL_NS:
            if (tlvU64(tlvs[i], &v)) sawWall = true;
            else eventMetadataValid = false;
            break;
        case WLR_TAG_MONO_NS:
            if (tlvU64(tlvs[i], &v)) sawMono = true;
            else eventMetadataValid = false;
            break;
        case WLR_TAG_ORIGIN_TID:
            if (tlvU64(tlvs[i], &v)) sawTid = true;
            else eventMetadataValid = false;
            break;
        case WLR_TAG_API_TYPE:
            if (tlvU64(tlvs[i], &v)) {
              EXPECT_LE(v, (uint64_t)UINT16_MAX)
                  << "frame " << frames << " has an out-of-range API type";
              if (v <= UINT16_MAX) {
                ev.apiType = static_cast<uint16_t>(v);
              } else {
                eventMetadataValid = false;
              }
            } else {
              eventMetadataValid = false;
            }
            break;
        case WLR_TAG_EVENT_PHASE:
            if (tlvU64(tlvs[i], &v)) {
              EXPECT_LE(v, (uint64_t)UINT16_MAX)
                  << "frame " << frames << " has an out-of-range event phase";
              if (v <= UINT16_MAX) {
                ev.phase = static_cast<uint16_t>(v);
              } else {
                eventMetadataValid = false;
              }
            } else {
              eventMetadataValid = false;
            }
            break;
        case WLR_TAG_CALL_ID:
            if (tlvU64(tlvs[i], &v)) {
              ev.callId = v;
            } else {
              eventMetadataValid = false;
            }
            break;
        default: break;
      }
    }

    if (fh.type == WLR_FRAME_EVENT) {
      EXPECT_TRUE(eventMetadataValid) << "frame " << frames << " has malformed event metadata";
      // Every event carries the common set, regardless of API.
      EXPECT_TRUE(sawWall) << "frame " << frames << " missing wall time";
      EXPECT_TRUE(sawMono) << "frame " << frames << " missing monotonic time";
      EXPECT_TRUE(sawTid) << "frame " << frames << " missing origin thread";
      if (eventMetadataValid) {
        EXPECT_NE(ev.apiType, 0) << "frame " << frames << " missing API type";
        EXPECT_NE(ev.callId, 0u) << "frame " << frames << " missing call ID";

        // And it must satisfy the shared required-tag table, which is the same table
        // the recorder is supposed to have encoded against.
        EXPECT_EQ(wlrValidateRequiredTags(fh.type, ev.apiType, ev.phase, tlvs, tlvCount),
                  WLR_DECODE_OK)
            << "frame " << frames << " (api " << ev.apiType << " phase " << ev.phase
            << ") fails the required-tag table";

        events.push_back(ev);
      }
    } else {
      EXPECT_EQ(wlrValidateRequiredTags(fh.type, WLR_API_UNKNOWN, 0, tlvs, tlvCount),
                WLR_DECODE_OK)
          << "frame " << frames << " (type " << fh.type << ") fails the required-tag table";
    }

    if (consumed == 0 || consumed > buf.size() - off) {
      ADD_FAILURE() << "frame " << frames << " decoded with invalid size " << consumed;
      break;
    }
    off += consumed;
    ++frames;
  }

  // A cleanly stopped session has no torn tail. A torn tail is legal for a copy of
  // an active file, so it is reported rather than failed.
  if (tail == WLR_DECODE_TORN_TAIL || tail == WLR_DECODE_MISSING_LF) {
    GTEST_LOG_(INFO) << "trailing partial frame (normal for an active-file copy)";
  } else if (tail != WLR_DECODE_OK) {
    ADD_FAILURE() << "mid-file decode error " << tail << " after " << frames << " frames";
  }

  ASSERT_GT(frames, 0) << "segment contains no frames -- hooks are not recording";
  GTEST_LOG_(INFO) << "decoded " << frames << " frames, " << events.size() << " events";

  // BEGIN must precede RETURN for each call ID: that ordering is the entire point
  // of the write-before-call guarantee, and it must be visible in the file.
  // Build the BEGIN index first, then validate every RETURN. This distinguishes a
  // RETURN whose BEGIN is in an earlier segment from one whose BEGIN appears later
  // in this segment (the latter is genuine ordering corruption).
  //
  // A RETURN whose BEGIN is absent is NOT a failure: wlrRotateIfNeeded rotates
  // between whole events, so a call's BEGIN can land in one segment and its RETURN
  // in the next. Validating a single file (which is all this helper is given, via
  // argv[1]) legitimately sees such a RETURN, and flagging it would report a
  // perfectly good segment as broken. What IS checkable within one file is the
  // ordering: when both phases are present, BEGIN must come first.
  std::unordered_map<uint64_t, uint64_t> beginSeq;
  int32_t                                orphanReturns = 0;
  for (const auto &ev : events) {
    if (ev.phase == WLR_PHASE_BEGIN) {
      /* Call IDs are minted monotonically, so two BEGINs for one ID cannot happen in
       * a sound recording. emplace() would quietly keep the first and discard the
       * second, and the RETURN check below would then validate against the wrong
       * sequence -- losing the corruption signal entirely. Report it instead. */
      auto ins = beginSeq.emplace(ev.callId, ev.eventSeq);
      EXPECT_TRUE(ins.second) << "call " << ev.callId << " has a duplicate BEGIN at eventSeq "
                              << ev.eventSeq << " (first was " << ins.first->second << ")";
    }
  }
  for (const auto &ev : events) {
    if (ev.phase != WLR_PHASE_RETURN) continue;
    auto it = beginSeq.find(ev.callId);
    if (it == beginSeq.end()) {
      ++orphanReturns; /* BEGIN is in an earlier segment; see above */
      continue;
    }
    EXPECT_LT(it->second, ev.eventSeq)
        << "call " << ev.callId << " has a RETURN ordered before its BEGIN";
  }
  if (orphanReturns > 0) {
    GTEST_LOG_(INFO) << orphanReturns
                     << " RETURN(s) whose BEGIN precedes this segment (expected after rotation)";
  }

  // Credentials are checked per-TLV in the frame walk above, so nothing is
  // scanned twice and no additional copy of the segment is made here.
}

namespace {

/*
 * Build a segment the same way the writer does: file header, then whole frames with
 * BEGIN/RETURN pairs. This is what makes the test self-contained -- it no longer
 * depends on a human running a recording first, so it has value in CI.
 *
 * It does NOT replace checking a real recording: this fixture proves the validator
 * and the encoder agree, while argv[1] proves the shipped recorder agrees with both.
 * Keeping the same validation function behind both is the point.
 *
 * Returns false on encode failure so buildFixtureSegment aborts with a clear error
 * rather than silently emitting a frame with a sequence-number gap.
 */
bool appendEvent(std::vector<uint8_t> *out, uint64_t seq, uint16_t apiType, uint16_t phase,
                 uint64_t callId, uint64_t connId, const char *sql, const char *user = nullptr) {
  uint8_t payload[1024];
  int32_t off = 0;
  int32_t n;

  n = wlrEncodeTlvU64(payload + off, sizeof(payload) - off, WLR_TAG_WALL_NS, true, 1700000000000000000ull);
  if (!advanceOffset(&off, n, sizeof(payload))) return false;
  n = wlrEncodeTlvU64(payload + off, sizeof(payload) - off, WLR_TAG_MONO_NS, true, 500ull + seq);
  if (!advanceOffset(&off, n, sizeof(payload))) return false;
  n = wlrEncodeTlvU64(payload + off, sizeof(payload) - off, WLR_TAG_ORIGIN_TID, true, 4242ull);
  if (!advanceOffset(&off, n, sizeof(payload))) return false;
  n = wlrEncodeTlvU64(payload + off, sizeof(payload) - off, WLR_TAG_API_TYPE, true, apiType);
  if (!advanceOffset(&off, n, sizeof(payload))) return false;
  n = wlrEncodeTlvU64(payload + off, sizeof(payload) - off, WLR_TAG_EVENT_PHASE, true, phase);
  if (!advanceOffset(&off, n, sizeof(payload))) return false;
  n = wlrEncodeTlvU64(payload + off, sizeof(payload) - off, WLR_TAG_CALL_ID, true, callId);
  if (!advanceOffset(&off, n, sizeof(payload))) return false;
  if (connId != 0) {
    n = wlrEncodeTlvU64(payload + off, sizeof(payload) - off, WLR_TAG_CONN_ID, true, connId);
    if (!advanceOffset(&off, n, sizeof(payload))) return false;
  }
  if (sql != nullptr) {
    n = wlrEncodeTlv(payload + off, sizeof(payload) - off, WLR_TAG_SQL, true, 0, sql,
                     (uint32_t)strlen(sql));
    if (!advanceOffset(&off, n, sizeof(payload))) return false;
  }
  if (user != nullptr) {
    n = wlrEncodeTlv(payload + off, sizeof(payload) - off, WLR_TAG_USER, true, 0, user,
                     (uint32_t)strlen(user));
    if (!advanceOffset(&off, n, sizeof(payload))) return false;
  }
  if (phase != WLR_PHASE_BEGIN) {
    n = wlrEncodeTlvU64(payload + off, sizeof(payload) - off, WLR_TAG_RET_CODE, true, 0);
    if (!advanceOffset(&off, n, sizeof(payload))) return false;
  }

  SWlrFrameHeader fh;
  memset(&fh, 0, sizeof(fh));
  fh.type = WLR_FRAME_EVENT;
  fh.eventSequence = seq;
  fh.logicalEventId = callId;
  fh.payloadLength = (uint32_t)off;

  std::vector<uint8_t> frame(wlrFrameEncodedSize(fh.payloadLength));
  int32_t              w = wlrEncodeFrame(frame.data(), frame.size(), &fh, payload);
  if (!requirePositive(w, "frame encoding")) return false;
  out->insert(out->end(), frame.begin(), frame.begin() + w);
  return true;
}

/*
 * nextSeq, when non-null, receives the first unused event sequence. A caller that
 * appends further events must take it from here rather than hardcoding a number:
 * eventSequence has to stay contiguous (validateSegmentBuffer checks it), so a
 * literal would silently collide or leave a gap the moment this fixture grows an
 * event, and the contiguity assertion would then fail for a reason that has nothing
 * to do with what the test is actually checking.
 */
bool buildFixtureSegment(std::vector<uint8_t> *out, uint64_t *nextSeq = nullptr) {
  std::vector<uint8_t> seg;

  SWlrFileHeader hdr;
  memset(&hdr, 0, sizeof(hdr));
  memset(hdr.sessionId, 0x11, sizeof(hdr.sessionId));
  hdr.processStartWallNs = 1700000000000000000ll;
  hdr.processStartMonoNs = 100;
  hdr.pid = 12345;
  hdr.segmentIndex = 0;
  hdr.firstEventSeq = 1;

  uint8_t hbuf[WLR_FILE_HEADER_SIZE];
  int32_t hn = wlrEncodeFileHeader(hbuf, sizeof(hbuf), &hdr);
  if (!requirePositive(hn, "file header encoding")) return false;
  seg.insert(seg.end(), hbuf, hbuf + hn);

  uint64_t seq = 1;
  // connect
  if (!appendEvent(&seg, seq++, WLR_API_CONNECT, WLR_PHASE_BEGIN, 1, 0, nullptr, "root")) return false;
  if (!appendEvent(&seg, seq++, WLR_API_CONNECT, WLR_PHASE_RETURN, 1, 1, nullptr)) return false;
  // two queries on that connection
  if (!appendEvent(&seg, seq++, WLR_API_QUERY, WLR_PHASE_BEGIN, 2, 1, "select 1")) return false;
  if (!appendEvent(&seg, seq++, WLR_API_QUERY, WLR_PHASE_RETURN, 2, 1, nullptr)) return false;
  if (!appendEvent(&seg, seq++, WLR_API_QUERY, WLR_PHASE_BEGIN, 3, 1, "insert into t values(now,1)")) {
    return false;
  }
  if (!appendEvent(&seg, seq++, WLR_API_QUERY, WLR_PHASE_RETURN, 3, 1, nullptr)) return false;

  if (nextSeq != nullptr) *nextSeq = seq;

  *out = std::move(seg);
  return true;
}

}  // namespace

// Runs in CI with no external input.
TEST(wlrSegmentRead, SynthesizedSegmentIsWellFormed) {
  taosResolveCRC();
  std::vector<uint8_t> seg;
  ASSERT_TRUE(buildFixtureSegment(&seg)) << "fixture failed to encode";
  ASSERT_GT(seg.size(), (size_t)WLR_FILE_HEADER_SIZE);
  validateSegmentBuffer(seg, "wlr_synthetic_credential_sentinel");
}

// A BEGIN whose RETURN never arrives is how a hung call is captured, so it must
// validate rather than look like damage (DS 6.17).
TEST(wlrSegmentRead, UnfinishedCallStillValidates) {
  taosResolveCRC();
  std::vector<uint8_t> seg;
  uint64_t nextSeq = 0;
  ASSERT_TRUE(buildFixtureSegment(&seg, &nextSeq)) << "fixture failed to encode";
  ASSERT_TRUE(appendEvent(&seg, nextSeq, WLR_API_QUERY, WLR_PHASE_BEGIN, 9, 1, "select never_returns"));
  validateSegmentBuffer(seg, "wlr_synthetic_credential_sentinel");
}

TEST(wlrSegmentRead, CredentialSentinelMatcherFindsLeaks) {
  const char value[] = "prefix-wlr_secret-suffix";
  SWlrTlv    tlv = {};
  tlv.value = reinterpret_cast<const uint8_t *>(value);
  tlv.length = static_cast<uint32_t>(sizeof(value) - 1);
  EXPECT_TRUE(credentialValueContains(tlv, "wlr_secret"));
  EXPECT_FALSE(credentialValueContains(tlv, "not_present"));
}

// Optional: validate a real recording produced by the shipped recorder.
TEST(wlrSegmentRead, RecordedSegmentIsWellFormed) {
  if (gSegmentPath == nullptr) {
    GTEST_SKIP() << "pass a segment path as argv[1] to check a real recording";
  }
  const char *credentialSentinel = std::getenv("WLR_TEST_CREDENTIAL_SENTINEL");
  ASSERT_NE(credentialSentinel, nullptr) << "WLR_TEST_CREDENTIAL_SENTINEL is required when validating a real segment";
  ASSERT_NE(credentialSentinel[0], '\0') << "WLR_TEST_CREDENTIAL_SENTINEL must not be empty";
  taosResolveCRC();
  std::vector<uint8_t> buf;
  ReadFileResult readResult = readFile(gSegmentPath, &buf);
  if (readResult == ReadFileResult::kTooLarge) {
    GTEST_SKIP() << gSegmentPath
                 << " is a legal oversized segment that this whole-file validator does not buffer";
  }
  ASSERT_EQ(readResult, ReadFileResult::kOk) << "cannot read " << gSegmentPath;
  validateSegmentBuffer(buf, credentialSentinel);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  if (argc > 1 && argv[1] != nullptr && argv[1][0] != '\0') gSegmentPath = argv[1];
  return RUN_ALL_TESTS();
}
