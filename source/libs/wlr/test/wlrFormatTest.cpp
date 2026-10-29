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
 * Workload Recorder (WLR) file format unit tests.
 *
 * Covers the implemented on-disk format cases in the DS 11.4 unit-test bucket,
 * including ENC002-ENC006, ENC009, ENC016-ENC018, FIL001, FIL006-FIL008,
 * FIL010, FIL014-FIL015, FMT003-FMT004, FMT008, and ORD010. Cases not listed
 * here belong to integration or client-hook tests rather than this codec suite.
 *
 * This file carries a REFERENCE encoder/decoder derived from DS 6.15.2, kept
 * alongside the shipped one on purpose. That reference is the executable
 * statement of the spec: it pins the frame layout, CRC coverage, trailing LF,
 * little-endian byte order and TLV rules. Because wlrFormat.h now exists and
 * carries an implementation, the include below picks the real one up and these
 * same tests run as a conformance suite against it -- so a divergence between
 * shipped code and spec shows up as a test failure. The reference is retained
 * for the case where the header is absent (it predates the implementation, and
 * the tests still state the spec without it).
 *
 * IMPORTANT: when wiring the real implementation, do not relax an assertion to
 * make it pass. Every constant here traces to a numbered DS clause cited inline.
 */

/* Windows headers define min/max as macros, which turns a std::min call into
 * std::( and fails to compile. Suppress them before anything can pull in
 * windows.h. */
#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#endif

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#if defined(__has_include)
#if __has_include("wlrFormat.h")
#define WLR_HAVE_REAL_HEADER 1
extern "C" {
#include "wlrFormat.h"
}
#endif
#endif

// The encode/decode functions declared in wlrFormat.h are only linkable when
// source/libs/wlr/src is part of the build. Without it the reference model below
// is what the tests exercise, and WlrHeaderMatchesReference asserts the header's
// constants agree with it -- so a spec drift in either direction fails the
// build's tests rather than passing silently.
#if defined(WLR_HAVE_REAL_HEADER) && defined(WLR_FORMAT_HAVE_IMPL)
#define WLR_CONFORMANCE 1
extern "C" {
#include "tcrc32c.h"
/* For the TSDB_DATA_TYPE_* codes a bind blob stores. Public API header only, the
 * same one the library and taosreplay use. */
#include "taos.h"
}
#endif

namespace {

// ---------------------------------------------------------------- DS 6.15.2

constexpr char     kMagic[8] = {'T', 'D', 'W', 'R', 'E', 'C', '0', '1'};
constexpr uint16_t kMajor = 1;
// Unreleased: everything added during development is part of 1.0, so there is
// no minor history to track yet.
constexpr uint16_t kMinor = 1;
constexpr uint16_t kEndianMark = 0x0102;
constexpr uint8_t  kDelimiter = 0x0A;
constexpr uint32_t kMaxFramePayload = 1u << 20;  // 1MiB, DS 6.15.2
constexpr uint32_t kChunkSize = 1u << 20;        // WORKLOAD_CHUNK_SIZE, DS 6.13
constexpr size_t   kFrameLengthSize = 4;
constexpr size_t   kFrameBodyHeaderSize = 24;
constexpr size_t   kFrameBodyPayloadLenOffset = kFrameBodyHeaderSize - sizeof(uint32_t);
constexpr size_t   kFrameCrcSize = 4;
constexpr size_t   kFrameDelimiterSize = 1;

// V1 tag numbers, DS 6.15.2 tag table. This is the single inventory used by
// the reference enum, parser-known list, and production-header conformance tests.
#define WLR_TEST_TAGS(X) \
  X(TAG_WALL_NS, WLR_TAG_WALL_NS, 1) \
  X(TAG_MONO_NS, WLR_TAG_MONO_NS, 2) \
  X(TAG_ORIGIN_TID, WLR_TAG_ORIGIN_TID, 3) \
  X(TAG_EMITTER_TID, WLR_TAG_EMITTER_TID, 4) \
  X(TAG_API_TYPE, WLR_TAG_API_TYPE, 5) \
  X(TAG_EVENT_PHASE, WLR_TAG_EVENT_PHASE, 6) \
  X(TAG_CALL_ID, WLR_TAG_CALL_ID, 7) \
  X(TAG_PARENT_CALL_ID, WLR_TAG_PARENT_CALL_ID, 8) \
  X(TAG_CONN_ID, WLR_TAG_CONN_ID, 9) \
  X(TAG_STMT_ID, WLR_TAG_STMT_ID, 10) \
  X(TAG_RESULT_ID, WLR_TAG_RESULT_ID, 11) \
  X(TAG_REQID, WLR_TAG_REQID, 12) \
  X(TAG_QID, WLR_TAG_QID, 13) \
  X(TAG_SQL, WLR_TAG_SQL, 100) \
  X(TAG_ENDPOINT, WLR_TAG_ENDPOINT, 101) \
  X(TAG_USER, WLR_TAG_USER, 102) \
  X(TAG_DB, WLR_TAG_DB, 103) \
  X(TAG_PORT, WLR_TAG_PORT, 104) \
  X(TAG_CONN_OPT_NAME, WLR_TAG_CONN_OPT_NAME, 110) \
  X(TAG_CONN_OPT_TYPE, WLR_TAG_CONN_OPT_TYPE, 111) \
  X(TAG_CONN_OPT_VALUE, WLR_TAG_CONN_OPT_VALUE, 112) \
  X(TAG_CONN_OPT_LIST, WLR_TAG_CONN_OPT_LIST, 113) \
  X(TAG_BIND_MODE, WLR_TAG_BIND_MODE, 120) \
  X(TAG_BIND_TYPE, WLR_TAG_BIND_TYPE, 121) \
  X(TAG_BIND_COUNT, WLR_TAG_BIND_COUNT, 122) \
  X(TAG_BIND_NULL, WLR_TAG_BIND_NULL, 123) \
  X(TAG_BIND_LENGTH, WLR_TAG_BIND_LENGTH, 124) \
  X(TAG_BIND_BYTES, WLR_TAG_BIND_BYTES, 125) \
  X(TAG_STMT_TBNAME, WLR_TAG_STMT_TBNAME, 126) \
  X(TAG_STMT2_TBNAMES, WLR_TAG_STMT2_TBNAMES, 127) \
  X(TAG_BIND_COL_IDX, WLR_TAG_BIND_COL_IDX, 128) \
  X(TAG_STMT2_ASYNC, WLR_TAG_STMT2_ASYNC, 129) \
  X(TAG_SML_PROTOCOL, WLR_TAG_SML_PROTOCOL, 130) \
  X(TAG_SML_PRECISION, WLR_TAG_SML_PRECISION, 131) \
  X(TAG_SML_TTL, WLR_TAG_SML_TTL, 132) \
  X(TAG_SML_COUNT, WLR_TAG_SML_COUNT, 133) \
  X(TAG_SML_TBNAME_KEY, WLR_TAG_SML_TBNAME_KEY, 134) \
  X(TAG_SML_PAYLOAD, WLR_TAG_SML_PAYLOAD, 135) \
  X(TAG_ACTIVE_CALL_COUNT, WLR_TAG_ACTIVE_CALL_COUNT, 136) \
  X(TAG_STMT2_TAGS, WLR_TAG_STMT2_TAGS, 137) \
  X(TAG_STMT_INIT_OPTIONS, WLR_TAG_STMT_INIT_OPTIONS, 138) \
  X(TAG_STMT2_NULLABLE_TBNAMES, WLR_TAG_STMT2_NULLABLE_TBNAMES, 139) \
  X(TAG_RET_CODE, WLR_TAG_RET_CODE, 140) \
  X(TAG_AFFECTED_ROWS, WLR_TAG_AFFECTED_ROWS, 141) \
  X(TAG_FETCH_ROWS, WLR_TAG_FETCH_ROWS, 142) \
  X(TAG_DURATION_NS, WLR_TAG_DURATION_NS, 143) \
  X(TAG_CHUNK_INDEX, WLR_TAG_CHUNK_INDEX, 150) \
  X(TAG_CHUNK_COUNT, WLR_TAG_CHUNK_COUNT, 151) \
  X(TAG_TOTAL_LEN, WLR_TAG_TOTAL_LENGTH, 152) \
  X(TAG_PAYLOAD_CRC, WLR_TAG_PAYLOAD_CRC, 153)

enum WlrTag : uint16_t {
#define WLR_DECLARE_TEST_TAG(referenceName, productionName, value) referenceName = value,
  WLR_TEST_TAGS(WLR_DECLARE_TEST_TAG)
#undef WLR_DECLARE_TEST_TAG
};

// Tag high bit marks "required" -- parser must reject an unknown required tag
// and skip an unknown optional tag by length. DS 6.15.2.
constexpr uint16_t kRequiredBit = 0x8000;

enum WlrFrameType : uint16_t {
  FRAME_EVENT = 1,
  FRAME_CHUNK_BEGIN = 2,
  FRAME_CHUNK_DATA = 3,
  FRAME_CHUNK_END = 4,
  FRAME_SESSION_STOP = 5,
  FRAME_FATAL = 6,
};

// ------------------------------------------------------- little-endian codec

// Explicit LE writers: the on-disk representation must not track host byte
// order (DS 6.15.2, TS ENC018). Writing through memcpy of a native integer
// would silently produce big-endian files on a BE host.
void putU16(std::vector<uint8_t>& b, uint16_t v) {
  b.push_back(static_cast<uint8_t>(v & 0xFF));
  b.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
}
void putU32(std::vector<uint8_t>& b, uint32_t v) {
  for (int i = 0; i < 4; ++i) b.push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xFF));
}
void putU64(std::vector<uint8_t>& b, uint64_t v) {
  for (int i = 0; i < 8; ++i) b.push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xFF));
}
uint16_t getU16(const uint8_t* p) { return static_cast<uint16_t>(p[0] | (p[1] << 8)); }
uint32_t getU32(const uint8_t* p) {
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i) v |= static_cast<uint32_t>(p[i]) << (8 * i);
  return v;
}
uint64_t getU64(const uint8_t* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(p[i]) << (8 * i);
  return v;
}

// CRC32C (Castagnoli), software table -- the reference only needs correctness,
// not speed. The shipped recorder uses the project's crc32c from tcrc32c.h.
uint32_t crc32cRef(uint32_t crc, const uint8_t* data, size_t len) {
  crc = ~crc;
  for (size_t i = 0; i < len; ++i) {
    crc ^= data[i];
    for (int k = 0; k < 8; ++k) {
      crc = (crc & 1) ? (crc >> 1) ^ 0x82F63B78u : (crc >> 1);
    }
  }
  return ~crc;
}

// ----------------------------------------------------------------- TLV model

struct Tlv {
  uint16_t             tag;
  uint16_t             flags;
  std::vector<uint8_t> value;

  bool required() const { return (tag & kRequiredBit) != 0; }
  uint16_t baseTag() const { return static_cast<uint16_t>(tag & ~kRequiredBit); }
};

Tlv tlvU64(uint16_t tag, uint64_t v, bool required = true) {
  Tlv t{static_cast<uint16_t>(required ? (tag | kRequiredBit) : tag), 0, {}};
  putU64(t.value, v);
  return t;
}

Tlv tlvBytes(uint16_t tag, const void* p, size_t n, bool required = true) {
  Tlv t{static_cast<uint16_t>(required ? (tag | kRequiredBit) : tag), 0, {}};
  const uint8_t* b = static_cast<const uint8_t*>(p);
  t.value.assign(b, b + n);
  return t;
}

Tlv tlvStr(uint16_t tag, const std::string& s, bool required = true) {
  // Strings carry an explicit length and are NOT NUL-terminated. DS 6.15.2.
  return tlvBytes(tag, s.data(), s.size(), required);
}

void encodeTlvs(std::vector<uint8_t>& out, const std::vector<Tlv>& tlvs) {
  for (const auto& t : tlvs) {
    putU16(out, t.tag);
    putU16(out, t.flags);
    putU32(out, static_cast<uint32_t>(t.value.size()));
    out.insert(out.end(), t.value.begin(), t.value.end());
  }
}

// ---------------------------------------------------------------- file header

struct FileHeader {
  uint8_t  sessionId[16];
  int64_t  processStartWallNs;
  int64_t  processStartMonoNs;
  uint64_t pid;
  uint32_t segmentIndex;
  uint64_t firstEventSeq;
  char     clockDomainId[80];
};

std::vector<uint8_t> encodeHeader(const FileHeader& h) {
  std::vector<uint8_t> b;
  b.insert(b.end(), kMagic, kMagic + sizeof(kMagic));
  putU16(b, kMajor);
  putU16(b, kMinor);
  putU16(b, kEndianMark);
  // headerLength is emitted after we know it; reserve then patch.
  size_t lenPos = b.size();
  putU16(b, 0);
  b.insert(b.end(), h.sessionId, h.sessionId + 16);
  putU64(b, static_cast<uint64_t>(h.processStartWallNs));
  putU64(b, static_cast<uint64_t>(h.processStartMonoNs));
  putU64(b, h.pid);
  putU32(b, h.segmentIndex);
  putU64(b, h.firstEventSeq);
  b.insert(b.end(), h.clockDomainId, h.clockDomainId + sizeof(h.clockDomainId));
  uint16_t total = static_cast<uint16_t>(b.size());
  b[lenPos] = static_cast<uint8_t>(total & 0xFF);
  b[lenPos + 1] = static_cast<uint8_t>((total >> 8) & 0xFF);
  return b;
}

// --------------------------------------------------------------------- frame

struct Frame {
  uint16_t             type;
  uint16_t             flags;
  uint64_t             eventSeq;
  uint64_t             logicalEventId;
  std::vector<uint8_t> payload;
};

// Layout per DS 6.15.2:
//   frameLength:u32 | frameType:u16 | flags:u16 | eventSequence:u64 |
//   logicalEventId:u64 | payloadLength:u32 | payload | crc32c:u32 | LF:u8
//
// frameLength counts from frameType through the trailing delimiter.
// CRC covers frameType through the last payload byte -- excluding frameLength,
// crc32c and the delimiter.
std::vector<uint8_t> encodeFrame(const Frame& f) {
  std::vector<uint8_t> body;
  putU16(body, f.type);
  putU16(body, f.flags);
  putU64(body, f.eventSeq);
  putU64(body, f.logicalEventId);
  putU32(body, static_cast<uint32_t>(f.payload.size()));
  body.insert(body.end(), f.payload.begin(), f.payload.end());

  uint32_t crc = crc32cRef(0, body.data(), body.size());

  // frameLength = body + crc(4) + delimiter(1)
  uint32_t frameLength = static_cast<uint32_t>(body.size() + 4 + 1);

  std::vector<uint8_t> out;
  putU32(out, frameLength);
  out.insert(out.end(), body.begin(), body.end());
  putU32(out, crc);
  out.push_back(kDelimiter);
  return out;
}

enum class DecodeResult {
  kOk,
  kTornTail,       // incomplete trailing frame: legal, ignore with warning
  kCrcMismatch,    // mid-file corruption: structurallyComplete=false
  kMissingLf,      // frame not terminated: treated as torn tail
  kBadLength,
};

struct DecodedFrame {
  Frame    frame;
  uint32_t consumed;
};

DecodeResult decodeFrame(const uint8_t* p, size_t avail, DecodedFrame* out) {
  if (avail < 4) return DecodeResult::kTornTail;
  uint32_t frameLength = getU32(p);
  // header fields after frameLength: 2+2+8+8+4 = 24, plus crc 4 + lf 1
  if (frameLength < kFrameBodyHeaderSize + kFrameCrcSize + kFrameDelimiterSize) {
    return DecodeResult::kBadLength;
  }
  // A frame never exceeds the payload cap plus framing overhead. Rejecting this
  // before the availability check matters: without it a hostile frameLength is
  // reported as a torn tail (recoverable) rather than malformed, which is the
  // opposite conclusion from the shipped wlrDecodeFrame.
  if (frameLength > kMaxFramePayload + kFrameBodyHeaderSize + kFrameCrcSize + kFrameDelimiterSize) {
    return DecodeResult::kBadLength;
  }
  if (avail < kFrameLengthSize + static_cast<size_t>(frameLength)) return DecodeResult::kTornTail;

  const uint8_t* body = p + kFrameLengthSize;
  size_t         bodyLen = frameLength - kFrameCrcSize - kFrameDelimiterSize;

  uint32_t payloadLength = getU32(body + kFrameBodyPayloadLenOffset);
  if (payloadLength > kMaxFramePayload) return DecodeResult::kBadLength;
  if (bodyLen != kFrameBodyHeaderSize + static_cast<size_t>(payloadLength)) return DecodeResult::kBadLength;

  uint32_t stored = getU32(body + bodyLen);
  uint32_t actual = crc32cRef(0, body, bodyLen);
  if (stored != actual) return DecodeResult::kCrcMismatch;

  if (p[kFrameLengthSize + frameLength - kFrameDelimiterSize] != kDelimiter) {
    return DecodeResult::kMissingLf;
  }

  out->frame.type = getU16(body);
  out->frame.flags = getU16(body + 2);
  out->frame.eventSeq = getU64(body + 4);
  out->frame.logicalEventId = getU64(body + 12);
  out->frame.payload.assign(body + kFrameBodyHeaderSize,
                            body + kFrameBodyHeaderSize + payloadLength);
  out->consumed = kFrameLengthSize + frameLength;
  return DecodeResult::kOk;
}

// TLV walk. Unknown required tag -> reject; unknown optional tag -> skip.
bool parseTlvs(const std::vector<uint8_t>& payload, const std::vector<uint16_t>& knownBaseTags,
               std::vector<Tlv>* out) {
  size_t off = 0;
  while (off < payload.size()) {
    if (payload.size() - off < 8) return false;
    Tlv t;
    t.tag = getU16(&payload[off]);
    t.flags = getU16(&payload[off + 2]);
    uint32_t len = getU32(&payload[off + 4]);
    off += 8;
    if (payload.size() - off < len) return false;
    t.value.assign(payload.begin() + off, payload.begin() + off + len);
    off += len;

    bool known = false;
    for (uint16_t k : knownBaseTags) {
      if (t.baseTag() == k) {
        known = true;
        break;
      }
    }
    if (!known && t.required()) return false;  // DS 6.15.2
    if (known) out->push_back(t);
  }
  return true;
}

std::vector<uint16_t> knownTags() {
  return {
#define WLR_REFERENCE_TAG_VALUE(referenceName, productionName, value) referenceName,
      WLR_TEST_TAGS(WLR_REFERENCE_TAG_VALUE)
#undef WLR_REFERENCE_TAG_VALUE
  };
}

Frame makeEventFrame(uint64_t seq, const std::vector<Tlv>& tlvs) {
  Frame f{FRAME_EVENT, 0, seq, seq, {}};
  encodeTlvs(f.payload, tlvs);
  return f;
}

}  // namespace

// ============================================================ ENC017 / ENC018

// TS ENC017 -- TLV encode/decode round-trip, byte-exact.
TEST(wlrFormat, TlvRoundTripIsByteExact) {
  const std::string sql = "select * from t1 where c1 > 3";
  std::vector<Tlv>  in{
      tlvU64(TAG_WALL_NS, 1753800000000000000ull),
      tlvU64(TAG_MONO_NS, 42ull),
      tlvU64(TAG_ORIGIN_TID, 7ull),
      tlvU64(TAG_CALL_ID, 99ull),
      tlvStr(TAG_SQL, sql),
  };

  Frame                f = makeEventFrame(1, in);
  std::vector<uint8_t> bytes = encodeFrame(f);

  DecodedFrame got;
  ASSERT_EQ(decodeFrame(bytes.data(), bytes.size(), &got), DecodeResult::kOk);
  EXPECT_EQ(got.consumed, bytes.size());
  EXPECT_EQ(got.frame.eventSeq, 1u);

  std::vector<Tlv> out;
  ASSERT_TRUE(parseTlvs(got.frame.payload, knownTags(), &out));
  ASSERT_EQ(out.size(), in.size());
  for (size_t i = 0; i < in.size(); ++i) {
    EXPECT_EQ(out[i].tag, in[i].tag) << "tag mismatch at " << i;
    EXPECT_EQ(out[i].flags, in[i].flags) << "flags mismatch at " << i;
    EXPECT_EQ(out[i].value, in[i].value) << "value mismatch at " << i;
  }

  // SQL must come back as plain UTF-8 with explicit length, not NUL-terminated.
  auto sqlIt = std::find_if(out.begin(), out.end(), [](const Tlv& tlv) {
    return tlv.baseTag() == TAG_SQL;
  });
  ASSERT_NE(sqlIt, out.end());
  std::string back(sqlIt->value.begin(), sqlIt->value.end());
  EXPECT_EQ(back, sql);
  EXPECT_EQ(back.size(), sql.size());
}

#ifdef WLR_CONFORMANCE
TEST(wlrFormat, EncoderRejectsTagWithRequiredBitAlreadySet) {
  uint8_t buf[WLR_TLV_HEADER_SIZE + 8] = {0};
  EXPECT_LT(wlrEncodeTlvU64(buf, sizeof(buf),
                            static_cast<uint16_t>(WLR_TAG_CALL_ID | WLR_TAG_REQUIRED_BIT), true, 1), 0);
  EXPECT_LT(wlrEncodeTlvU64(buf, sizeof(buf),
                            static_cast<uint16_t>(WLR_TAG_CALL_ID | WLR_TAG_REQUIRED_BIT), false, 1), 0);
  EXPECT_LT(wlrEncodeTlvU64(buf, sizeof(buf), 999, true, 1), 0);
  EXPECT_LT(wlrEncodeTlvU64(buf, sizeof(buf), 999, false, 1), 0);
}
#endif

// TS ENC018 -- on-disk integers are little-endian regardless of host order.
// A round-trip test cannot catch this: two same-endian hosts agree with each
// other while producing files unreadable elsewhere.
TEST(wlrFormat, IntegersAreLittleEndianOnDisk) {
  Frame                f = makeEventFrame(0x0102030405060708ull, {tlvU64(TAG_MONO_NS, 1)});
  std::vector<uint8_t> bytes = encodeFrame(f);

  // eventSequence starts at offset 4(frameLength) + 2(type) + 2(flags) = 8.
  const uint8_t* seq = bytes.data() + 8;
  EXPECT_EQ(seq[0], 0x08) << "eventSequence must be little-endian on disk";
  EXPECT_EQ(seq[1], 0x07);
  EXPECT_EQ(seq[2], 0x06);
  EXPECT_EQ(seq[3], 0x05);
  EXPECT_EQ(seq[4], 0x04);
  EXPECT_EQ(seq[5], 0x03);
  EXPECT_EQ(seq[6], 0x02);
  EXPECT_EQ(seq[7], 0x01);
}

TEST(wlrFormat, HeaderEndianMarkAndMagic) {
  FileHeader h{};
  std::memset(h.sessionId, 0xAB, sizeof(h.sessionId));
  h.pid = 4242;
  h.segmentIndex = 3;
  h.firstEventSeq = 100;
  std::vector<uint8_t> b = encodeHeader(h);

  // FIL001 -- self-describing header
  EXPECT_EQ(std::memcmp(b.data(), kMagic, 8), 0);
  EXPECT_EQ(getU16(b.data() + 8), kMajor);
  EXPECT_EQ(getU16(b.data() + 10), kMinor);
  // The endian mark is itself stored LE, so 0x0102 reads back as 02 01.
  EXPECT_EQ(b[12], 0x02);
  EXPECT_EQ(b[13], 0x01);
  EXPECT_EQ(getU16(b.data() + 14), static_cast<uint16_t>(b.size()));
}

// =================================================== ENC002 / ENC003 / FIL007

// TS ENC002 + FIL007 -- LF terminates every frame but is NOT a record
// separator. A payload containing 0x0A must not disturb parsing.
TEST(wlrFormat, PayloadNewlinesDoNotSplitFrames) {
  const std::string sql = "select 1\nfrom t1\n-- trailing comment\n";
  ASSERT_NE(sql.find('\n'), std::string::npos);

  std::vector<uint8_t> stream;
  for (uint64_t seq = 1; seq <= 3; ++seq) {
    Frame f = makeEventFrame(seq, {tlvU64(TAG_MONO_NS, seq), tlvStr(TAG_SQL, sql)});
    std::vector<uint8_t> b = encodeFrame(f);
    stream.insert(stream.end(), b.begin(), b.end());
  }

  // Counting LF bytes would over-count: the payload contributes its own.
  size_t lfCount = 0;
  for (uint8_t c : stream) {
    if (c == kDelimiter) ++lfCount;
  }
  EXPECT_GT(lfCount, 3u) << "test is only meaningful when payload adds newlines";

  // Length-driven parsing still yields exactly 3 frames.
  size_t   off = 0;
  uint64_t expected = 1;
  while (off < stream.size()) {
    DecodedFrame got;
    ASSERT_EQ(decodeFrame(stream.data() + off, stream.size() - off, &got), DecodeResult::kOk);
    EXPECT_EQ(got.frame.eventSeq, expected++);
    std::vector<Tlv> tlvs;
    ASSERT_TRUE(parseTlvs(got.frame.payload, knownTags(), &tlvs));
    auto sqlIt = std::find_if(tlvs.begin(), tlvs.end(), [](const Tlv& tlv) {
      return tlv.baseTag() == TAG_SQL;
    });
    ASSERT_NE(sqlIt, tlvs.end());
    std::string back(sqlIt->value.begin(), sqlIt->value.end());
    EXPECT_EQ(back, sql);
    off += got.consumed;
  }
  EXPECT_EQ(expected, 4u);
}

// TS ENC003 -- NUL bytes and non-UTF8 sequences survive by length.
TEST(wlrFormat, BinarySafePayloadWithNulAndHighBytes) {
  std::string blob;
  blob.push_back('a');
  blob.push_back('\0');
  blob.push_back('\xFF');
  blob.push_back('\xFE');
  blob.push_back('\n');
  blob.push_back('z');
  ASSERT_EQ(blob.size(), 6u);

  Frame f = makeEventFrame(1, {tlvBytes(TAG_BIND_BYTES, blob.data(), blob.size())});
  std::vector<uint8_t> bytes = encodeFrame(f);

  DecodedFrame got;
  ASSERT_EQ(decodeFrame(bytes.data(), bytes.size(), &got), DecodeResult::kOk);
  std::vector<Tlv> tlvs;
  ASSERT_TRUE(parseTlvs(got.frame.payload, knownTags(), &tlvs));
  ASSERT_EQ(tlvs.size(), 1u);
  ASSERT_EQ(tlvs[0].value.size(), blob.size());
  EXPECT_EQ(std::memcmp(tlvs[0].value.data(), blob.data(), blob.size()), 0);
}

// ==================================================== FIL006 / FIL008 / FIL010

// TS FIL008 -- CRC covers frameType..payload end only.
TEST(wlrFormat, CrcCoversBodyOnly) {
  Frame                f = makeEventFrame(5, {tlvU64(TAG_MONO_NS, 5)});
  std::vector<uint8_t> good = encodeFrame(f);

  // Flipping a payload byte must be detected.
  {
    std::vector<uint8_t> bad = good;
    const size_t payloadOffset = kFrameLengthSize + kFrameBodyHeaderSize;
    ASSERT_GT(bad.size(), payloadOffset);
    bad[payloadOffset] ^= 0xFF;
    DecodedFrame got;
    EXPECT_EQ(decodeFrame(bad.data(), bad.size(), &got), DecodeResult::kCrcMismatch);
  }
  // Flipping frameType must be detected (it is inside CRC coverage).
  {
    std::vector<uint8_t> bad = good;
    bad[4] ^= 0xFF;
    DecodedFrame got;
    EXPECT_EQ(decodeFrame(bad.data(), bad.size(), &got), DecodeResult::kCrcMismatch);
  }
}

// TS FIL006 -- every complete frame ends with LF; a missing LF is a torn frame.
TEST(wlrFormat, MissingTrailingLfIsTornNotCorrupt) {
  Frame                f = makeEventFrame(1, {tlvU64(TAG_MONO_NS, 1)});
  std::vector<uint8_t> bytes = encodeFrame(f);
  EXPECT_EQ(bytes.back(), kDelimiter);

  bytes.back() = 0x00;
  DecodedFrame got;
  // Distinct from kCrcMismatch: this must not mark the session structurally
  // incomplete, it is an unterminated tail frame. DS 6.15.2 / FS 4.9.
  EXPECT_EQ(decodeFrame(bytes.data(), bytes.size(), &got), DecodeResult::kMissingLf);
}

// TS FIL010 -- a copy of an active segment ends in a half frame; that prefix is
// usable and the tail is ignored, NOT reported as mid-file corruption.
TEST(wlrFormat, TornTailPrefixIsRecoverable) {
  std::vector<uint8_t> stream;
  for (uint64_t seq = 1; seq <= 4; ++seq) {
    Frame                f = makeEventFrame(seq, {tlvU64(TAG_MONO_NS, seq)});
    std::vector<uint8_t> b = encodeFrame(f);
    stream.insert(stream.end(), b.begin(), b.end());
  }
  size_t fullSize = stream.size();

  // Simulate copying mid-append: chop the last frame in half.
  std::vector<uint8_t> torn(stream.begin(), stream.begin() + fullSize - 12);

  size_t   off = 0;
  uint64_t last = 0;
  DecodeResult tail = DecodeResult::kOk;
  while (off < torn.size()) {
    DecodedFrame got;
    DecodeResult r = decodeFrame(torn.data() + off, torn.size() - off, &got);
    if (r != DecodeResult::kOk) {
      tail = r;
      break;
    }
    last = got.frame.eventSeq;
    off += got.consumed;
  }
  EXPECT_EQ(tail, DecodeResult::kTornTail) << "active-file copy must end in a torn tail";
  EXPECT_EQ(last, 3u) << "three complete frames should be recoverable";
}

// TS FIL014 -- mid-file CRC error is different in kind from a torn tail.
TEST(wlrFormat, MidFileCorruptionIsDistinguishedFromTornTail) {
  std::vector<uint8_t> stream;
  std::vector<size_t>  starts;
  for (uint64_t seq = 1; seq <= 3; ++seq) {
    starts.push_back(stream.size());
    Frame                f = makeEventFrame(seq, {tlvU64(TAG_MONO_NS, seq)});
    std::vector<uint8_t> b = encodeFrame(f);
    stream.insert(stream.end(), b.begin(), b.end());
  }
  // Corrupt inside the SECOND frame's payload.
  stream[starts[1] + 30] ^= 0xFF;

  DecodedFrame got;
  ASSERT_EQ(decodeFrame(stream.data() + starts[0], stream.size() - starts[0], &got),
            DecodeResult::kOk);
  EXPECT_EQ(decodeFrame(stream.data() + starts[1], stream.size() - starts[1], &got),
            DecodeResult::kCrcMismatch);
}

// TS FIL015 -- eventSeq gaps are detectable by the reader.
TEST(wlrFormat, EventSequenceGapIsDetectable) {
  std::vector<uint64_t> seqs{1, 2, 4};  // 3 is missing
  std::vector<uint8_t>  stream;
  for (uint64_t s : seqs) {
    Frame                f = makeEventFrame(s, {tlvU64(TAG_MONO_NS, s)});
    std::vector<uint8_t> b = encodeFrame(f);
    stream.insert(stream.end(), b.begin(), b.end());
  }

  size_t   off = 0;
  uint64_t prev = 0;
  bool     gap = false;
  while (off < stream.size()) {
    DecodedFrame got;
    ASSERT_EQ(decodeFrame(stream.data() + off, stream.size() - off, &got), DecodeResult::kOk);
    if (prev != 0 && got.frame.eventSeq != prev + 1) gap = true;
    prev = got.frame.eventSeq;
    off += got.consumed;
  }
  EXPECT_TRUE(gap) << "reader must notice a missing eventSeq";
}

// ================================================================ ENC016 tags

// TS ENC016 -- unknown optional tag skipped, unknown required tag rejected.
TEST(wlrFormat, UnknownOptionalTagSkippedRequiredRejected) {
  const uint16_t kFutureTag = 900;

  // Optional: forward compatible.
  {
    std::vector<Tlv> tlvs{tlvU64(TAG_MONO_NS, 1), tlvU64(kFutureTag, 7, /*required=*/false)};
    Frame            f = makeEventFrame(1, tlvs);
    std::vector<uint8_t> bytes = encodeFrame(f);
    DecodedFrame     got;
    ASSERT_EQ(decodeFrame(bytes.data(), bytes.size(), &got), DecodeResult::kOk);
    std::vector<Tlv> out;
    EXPECT_TRUE(parseTlvs(got.frame.payload, knownTags(), &out));
    EXPECT_EQ(out.size(), 1u) << "unknown optional tag should be skipped, not kept";
  }
  // Required: must reject.
  {
    std::vector<Tlv> tlvs{tlvU64(TAG_MONO_NS, 1), tlvU64(kFutureTag, 7, /*required=*/true)};
    Frame            f = makeEventFrame(1, tlvs);
    std::vector<uint8_t> bytes = encodeFrame(f);
    DecodedFrame     got;
    ASSERT_EQ(decodeFrame(bytes.data(), bytes.size(), &got), DecodeResult::kOk);
    std::vector<Tlv> out;
    EXPECT_FALSE(parseTlvs(got.frame.payload, knownTags(), &out))
        << "unknown REQUIRED tag must be rejected";
  }
}

// ======================================================= FMT008 length limits

// TS FMT008 / SEC006 -- hostile lengths must be rejected, not trusted.
TEST(wlrFormat, HostileLengthsRejected) {
  Frame                f = makeEventFrame(1, {tlvU64(TAG_MONO_NS, 1)});
  std::vector<uint8_t> bytes = encodeFrame(f);

  // payloadLength claiming more than the 1MiB cap.
  {
    std::vector<uint8_t> bad = bytes;
    uint32_t             huge = kMaxFramePayload + 1;
    for (int i = 0; i < 4; ++i) bad[4 + 20 + i] = static_cast<uint8_t>((huge >> (8 * i)) & 0xFF);
    DecodedFrame got;
    EXPECT_EQ(decodeFrame(bad.data(), bad.size(), &got), DecodeResult::kBadLength);
  }
  // frameLength below the structural minimum.
  {
    std::vector<uint8_t> bad = bytes;
    bad[0] = 3;
    bad[1] = bad[2] = bad[3] = 0;
    DecodedFrame got;
    EXPECT_EQ(decodeFrame(bad.data(), bad.size(), &got), DecodeResult::kBadLength);
  }
  // frameLength above the payload cap plus framing overhead must be malformed,
  // not reported as a recoverable torn tail.
  {
    std::vector<uint8_t> bad = bytes;
    uint32_t huge = kMaxFramePayload + kFrameBodyHeaderSize + kFrameCrcSize +
                    kFrameDelimiterSize + 1;
    for (int i = 0; i < 4; ++i) {
      bad[i] = static_cast<uint8_t>((huge >> (8 * i)) & 0xFF);
    }
    DecodedFrame got;
    EXPECT_EQ(decodeFrame(bad.data(), bad.size(), &got), DecodeResult::kBadLength);
  }
  // payloadLength inconsistent with frameLength.
  {
    std::vector<uint8_t> bad = bytes;
    uint32_t             wrong = 999;
    for (int i = 0; i < 4; ++i) bad[4 + 20 + i] = static_cast<uint8_t>((wrong >> (8 * i)) & 0xFF);
    DecodedFrame got;
    EXPECT_EQ(decodeFrame(bad.data(), bad.size(), &got), DecodeResult::kBadLength);
  }
}

#ifdef WLR_CONFORMANCE
TEST(wlrFormat, FrameDecoderRejectsNullArgumentsAsBadArg) {
  uint8_t bytes[WLR_FRAME_MIN_LENGTH] = {};
  SWlrFrameHeader header = {};
  uint32_t consumed = 0;
  EXPECT_EQ(wlrDecodeFrame(nullptr, sizeof(bytes), &header, nullptr, &consumed), WLR_DECODE_BAD_ARG);
  EXPECT_EQ(wlrDecodeFrame(bytes, sizeof(bytes), nullptr, nullptr, &consumed), WLR_DECODE_BAD_ARG);
  EXPECT_EQ(wlrDecodeFrame(bytes, sizeof(bytes), &header, nullptr, nullptr), WLR_DECODE_BAD_ARG);
}
#endif

// TS FMT003 -- unsupported major version rejected; FMT004 minor accepted.
TEST(wlrFormat, MajorVersionGate) {
#ifdef WLR_CONFORMANCE
  EXPECT_TRUE(wlrVersionSupported(1, 0));
  EXPECT_TRUE(wlrVersionSupported(1, 7)) << "higher minor must stay readable";
  EXPECT_FALSE(wlrVersionSupported(2, 0)) << "unsupported major must be rejected";
  EXPECT_FALSE(wlrVersionSupported(0, 9));
#else
  GTEST_SKIP() << "the production version gate is unavailable without wlrFormat.h";
#endif
}

// ==================================================== ORD010 chunk reassembly

// TS ORD010 / ENC004 -- a logical payload larger than one frame is split into
// CHUNK_BEGIN / CHUNK_DATA* / CHUNK_END and reassembles byte-exactly.
TEST(wlrFormat, ChunkedPayloadReassembly) {
  // Deliberately not a chunk multiple, so the last chunk is partial.
  const size_t         total = kChunkSize * 2 + 12345;
  std::vector<uint8_t> big(total);
  for (size_t i = 0; i < total; ++i) big[i] = static_cast<uint8_t>((i * 31 + 7) & 0xFF);
  uint32_t wholeCrc = crc32cRef(0, big.data(), big.size());

  const uint64_t logicalId = 777;
  uint32_t       chunkCount = static_cast<uint32_t>((total + kChunkSize - 1) / kChunkSize);

  std::vector<uint8_t> stream;
  uint64_t             seq = 1;
  {
    Frame f{FRAME_CHUNK_BEGIN, 0, seq++, logicalId, {}};
    encodeTlvs(f.payload, {tlvU64(TAG_TOTAL_LEN, total), tlvU64(TAG_CHUNK_COUNT, chunkCount)});
    auto b = encodeFrame(f);
    stream.insert(stream.end(), b.begin(), b.end());
  }
  for (uint32_t i = 0; i < chunkCount; ++i) {
    size_t off = static_cast<size_t>(i) * kChunkSize;
    size_t remaining = total - off;
    size_t n = (static_cast<size_t>(kChunkSize) < remaining) ? static_cast<size_t>(kChunkSize)
                                                             : remaining;
    // A chunk's data must fit one frame payload. DS 6.13/6.15.2.
    ASSERT_LE(n, kMaxFramePayload);
    Frame f{FRAME_CHUNK_DATA, 0, seq++, logicalId, {}};
    f.payload.assign(big.begin() + off, big.begin() + off + n);
    auto b = encodeFrame(f);
    stream.insert(stream.end(), b.begin(), b.end());
  }
  {
    Frame f{FRAME_CHUNK_END, 0, seq++, logicalId, {}};
    encodeTlvs(f.payload, {tlvU64(TAG_PAYLOAD_CRC, wholeCrc)});
    auto b = encodeFrame(f);
    stream.insert(stream.end(), b.begin(), b.end());
  }

  // Reassemble.
  std::vector<uint8_t> rebuilt;
  uint64_t             seenTotal = 0, seenCount = 0, seenCrc = 0;
  bool                 sawEnd = false;
  size_t               off = 0;
  uint64_t             prevSeq = 0;
  while (off < stream.size()) {
    DecodedFrame got;
    ASSERT_EQ(decodeFrame(stream.data() + off, stream.size() - off, &got), DecodeResult::kOk);
    EXPECT_EQ(got.frame.eventSeq, prevSeq + 1) << "chunk frames must be contiguous";
    prevSeq = got.frame.eventSeq;
    EXPECT_EQ(got.frame.logicalEventId, logicalId);

    if (got.frame.type == FRAME_CHUNK_BEGIN) {
      std::vector<Tlv> tlvs;
      ASSERT_TRUE(parseTlvs(got.frame.payload, knownTags(), &tlvs));
      ASSERT_EQ(tlvs.size(), 2u);
      seenTotal = getU64(tlvs[0].value.data());
      seenCount = getU64(tlvs[1].value.data());
    } else if (got.frame.type == FRAME_CHUNK_DATA) {
      rebuilt.insert(rebuilt.end(), got.frame.payload.begin(), got.frame.payload.end());
    } else if (got.frame.type == FRAME_CHUNK_END) {
      std::vector<Tlv> tlvs;
      ASSERT_TRUE(parseTlvs(got.frame.payload, knownTags(), &tlvs));
      ASSERT_EQ(tlvs.size(), 1u);
      seenCrc = getU64(tlvs[0].value.data());
      sawEnd = true;
    }
    off += got.consumed;
  }

  ASSERT_TRUE(sawEnd) << "CHUNK_END must be present";
  EXPECT_EQ(seenTotal, total);
  EXPECT_EQ(seenCount, chunkCount);
  ASSERT_EQ(rebuilt.size(), big.size());
  EXPECT_EQ(std::memcmp(rebuilt.data(), big.data(), big.size()), 0);
  EXPECT_EQ(static_cast<uint32_t>(seenCrc), wholeCrc);
}

// A truncated chunk run must not reassemble as if complete.
TEST(wlrFormat, IncompleteChunkRunIsNotAccepted) {
  const size_t         total = kChunkSize + 100;
  std::vector<uint8_t> big(total, 0xAB);
  const uint64_t       logicalId = 5;
  uint32_t             chunkCount = 2;

  std::vector<uint8_t> stream;
  uint64_t             seq = 1;
  {
    Frame f{FRAME_CHUNK_BEGIN, 0, seq++, logicalId, {}};
    encodeTlvs(f.payload, {tlvU64(TAG_TOTAL_LEN, total), tlvU64(TAG_CHUNK_COUNT, chunkCount)});
    auto b = encodeFrame(f);
    stream.insert(stream.end(), b.begin(), b.end());
  }
  {
    // Only the FIRST data chunk, then the stream stops (writer died).
    Frame f{FRAME_CHUNK_DATA, 0, seq++, logicalId, {}};
    f.payload.assign(big.begin(), big.begin() + kChunkSize);
    auto b = encodeFrame(f);
    stream.insert(stream.end(), b.begin(), b.end());
  }

  size_t   off = 0;
  uint64_t declaredTotal = 0;
  size_t   got_bytes = 0;
  bool     sawEnd = false;
  while (off < stream.size()) {
    DecodedFrame got;
    ASSERT_EQ(decodeFrame(stream.data() + off, stream.size() - off, &got), DecodeResult::kOk);
    if (got.frame.type == FRAME_CHUNK_BEGIN) {
      std::vector<Tlv> tlvs;
      ASSERT_TRUE(parseTlvs(got.frame.payload, knownTags(), &tlvs));
      declaredTotal = getU64(tlvs[0].value.data());
    } else if (got.frame.type == FRAME_CHUNK_DATA) {
      got_bytes += got.frame.payload.size();
    } else if (got.frame.type == FRAME_CHUNK_END) {
      sawEnd = true;
    }
    off += got.consumed;
  }
  EXPECT_FALSE(sawEnd);
  EXPECT_LT(got_bytes, declaredTotal) << "partial chunk run must be visible as short";
}

// =============================================== header/reference conformance

#ifdef WLR_CONFORMANCE

// TS ENC005/ENC006 -- fixed and variable width columns, NULLs, multi-element.
TEST(wlrFormatConformance, BindRoundTripFixedAndVarLen) {
  // INT column: 3 elements, middle one NULL.
  int32_t     ints[3] = {10, 0, 30};
  uint8_t     intNull[3] = {0, 1, 0};
  SWlrBindEncodeCol c0 = {};
  c0.type = TSDB_DATA_TYPE_INT;
  c0.elemCount = 3;
  c0.isNull = intNull;
  c0.values = reinterpret_cast<const uint8_t*>(ints);
  c0.valueBytes = sizeof(ints);

  // VARCHAR column: 2 elements of differing length.
  const char  strs[] = "abcdefg";  // "abc" + "defg"
  int32_t     lens[2] = {3, 4};
  uint8_t     strNull[2] = {0, 0};
  SWlrBindEncodeCol c1 = {};
  c1.type = TSDB_DATA_TYPE_VARCHAR;
  c1.elemCount = 2;
  c1.isNull = strNull;
  c1.lengths = lens;
  c1.values = reinterpret_cast<const uint8_t*>(strs);
  c1.valueBytes = 7;

  SWlrBindEncodeCol in[2] = {c0, c1};
  uint32_t    need = wlrBindEncodedSize(in, 2);
  ASSERT_GT(need, 0u);

  std::vector<uint8_t> buf(need);
  int32_t              n = wlrEncodeBind(buf.data(), buf.size(), in, 2);
  ASSERT_GT(n, 0);
  EXPECT_LE(static_cast<uint32_t>(n), need) << "size estimate must not undercount";

  SWlrBindCol out[4];
  int32_t     count = 0;
  ASSERT_EQ(wlrDecodeBind(buf.data(), static_cast<uint32_t>(n), out, 4, &count), WLR_DECODE_OK);
  ASSERT_EQ(count, 2);
  EXPECT_EQ(wlrDecodeBind(buf.data(), static_cast<uint32_t>(n), out, 1, &count),
            WLR_DECODE_NO_SPACE);
  EXPECT_EQ(wlrDecodeBind(buf.data(), static_cast<uint32_t>(n), nullptr, 1, &count),
            WLR_DECODE_BAD_ARG);
  EXPECT_EQ(wlrDecodeBind(buf.data(), static_cast<uint32_t>(n), out, -1, &count),
            WLR_DECODE_BAD_ARG);

  EXPECT_EQ(out[0].type, TSDB_DATA_TYPE_INT);
  EXPECT_EQ(out[0].elemCount, 3);
  EXPECT_EQ(out[0].lengths, nullptr) << "fixed-width column must not carry a length array";
  EXPECT_EQ(out[0].isNull[1], 1) << "NULL flag lost";
  EXPECT_EQ(std::memcmp(out[0].values, ints, sizeof(ints)), 0);

  EXPECT_EQ(out[1].type, TSDB_DATA_TYPE_VARCHAR);
  EXPECT_EQ(out[1].elemCount, 2);
  ASSERT_NE(out[1].lengths, nullptr) << "var-length column needs per-element lengths";
  EXPECT_EQ(wlrBindColLength(&out[1], 0), 3u);
  EXPECT_EQ(wlrBindColLength(&out[1], 1), 4u);
  EXPECT_EQ(std::memcmp(out[1].values, strs, 7), 0);
}

// A caller that supplies no is_null array means "none are null", and that must be
// recorded explicitly rather than left for replay to infer.
TEST(wlrFormatConformance, BindNullArrayAbsenceIsExplicit) {
  int64_t     v[2] = {1, 2};
  SWlrBindEncodeCol c = {};
  c.type = TSDB_DATA_TYPE_BIGINT;
  c.elemCount = 2;
  c.isNull = nullptr;
  c.values = reinterpret_cast<const uint8_t*>(v);
  c.valueBytes = sizeof(v);

  std::vector<uint8_t> buf(wlrBindEncodedSize(&c, 1));
  int32_t              n = wlrEncodeBind(buf.data(), buf.size(), &c, 1);
  ASSERT_GT(n, 0);

  SWlrBindCol out[1];
  int32_t     count = 0;
  ASSERT_EQ(wlrDecodeBind(buf.data(), static_cast<uint32_t>(n), out, 1, &count), WLR_DECODE_OK);
  ASSERT_EQ(count, 1);
  ASSERT_NE(out[0].isNull, nullptr) << "null flags must always be present on the wire";
  EXPECT_EQ(out[0].isNull[0], 0);
  EXPECT_EQ(out[0].isNull[1], 0);
}

TEST(wlrFormatConformance, BindTriStateNullFlagsRoundTrip) {
  uint8_t nullStates[3] = {0, 1, 2};
  int32_t lengths[3] = {0, 0, 0};
  SWlrBindEncodeCol col = {};
  col.type = TSDB_DATA_TYPE_VARCHAR;
  col.flags = WLR_BIND_FLAG_TRISTATE_NULL;
  col.elemCount = 3;
  col.isNull = nullStates;
  col.lengths = lengths;

  std::vector<uint8_t> buf(wlrBindEncodedSize(&col, 1));
  int32_t n = wlrEncodeBind(buf.data(), buf.size(), &col, 1);
  ASSERT_GT(n, 0);

  SWlrBindCol out[1] = {};
  int32_t count = 0;
  ASSERT_EQ(wlrDecodeBind(buf.data(), static_cast<uint32_t>(n), out, 1, &count), WLR_DECODE_OK);
  ASSERT_EQ(count, 1);
  EXPECT_EQ(out[0].flags, WLR_BIND_FLAG_TRISTATE_NULL);
  EXPECT_EQ(out[0].isNull[0], 0);
  EXPECT_EQ(out[0].isNull[1], 1);
  EXPECT_EQ(out[0].isNull[2], 2);
}

// A variable-length column is only recoverable if the per-element lengths
// delimit the value bytes. Each case below would otherwise encode "successfully"
// while losing or misdescribing the data, which is worse than dropping the call:
// replay would rebuild empty or wildly-sized elements and report no problem.
TEST(wlrFormatConformance, BindRejectsVarLenWithUndelimitedValues) {
  const char *vals = "abcdefgh";
  uint8_t     buf[256];

  // No lengths array at all: nothing delimits the 8 value bytes.
  {
    SWlrBindEncodeCol c = {};
    c.type = TSDB_DATA_TYPE_VARCHAR;
    c.elemCount = 2;
    c.lengths = nullptr;
    c.values = reinterpret_cast<const uint8_t*>(vals);
    c.valueBytes = 8;
    EXPECT_LT(wlrEncodeBind(buf, sizeof(buf), &c, 1), 0)
        << "var-length values with no length array must be refused";
  }

  {
    SWlrBindEncodeCol c = {};
    c.type = TSDB_DATA_TYPE_VARCHAR;
    c.elemCount = 2;
    c.valueBytes = 0;
    EXPECT_LT(wlrEncodeBind(buf, sizeof(buf), &c, 1), 0)
        << "empty non-NULL values still require an explicit length array";
  }

  // Negative length: the wire format pins lengths as u32, so a cast would record
  // -1 as 4294967295 and describe a value extent far beyond the buffer.
  {
    int32_t     lens[2] = {-1, 2};
    SWlrBindEncodeCol c = {};
    c.type = TSDB_DATA_TYPE_VARCHAR;
    c.elemCount = 2;
    c.lengths = lens;
    c.values = reinterpret_cast<const uint8_t*>(vals);
    c.valueBytes = 4;
    EXPECT_LT(wlrEncodeBind(buf, sizeof(buf), &c, 1), 0)
        << "negative element length must be refused, not cast to a huge u32";
  }

  // Lengths that do not sum to valueBytes: the two disagree about the extent.
  {
    int32_t     lens[2] = {2, 2};
    SWlrBindEncodeCol c = {};
    c.type = TSDB_DATA_TYPE_VARCHAR;
    c.elemCount = 2;
    c.lengths = lens;
    c.values = reinterpret_cast<const uint8_t*>(vals);
    c.valueBytes = 8;  // lengths sum to 4
    EXPECT_LT(wlrEncodeBind(buf, sizeof(buf), &c, 1), 0)
        << "summed lengths must equal valueBytes";
  }

  // Control: a consistent var-length column still encodes and round-trips.
  {
    int32_t     lens[2] = {3, 5};
    SWlrBindEncodeCol c = {};
    c.type = TSDB_DATA_TYPE_VARCHAR;
    c.elemCount = 2;
    c.lengths = lens;
    c.values = reinterpret_cast<const uint8_t*>(vals);
    c.valueBytes = 8;
    int32_t n = wlrEncodeBind(buf, sizeof(buf), &c, 1);
    ASSERT_GT(n, 0) << "a consistent var-length column must still be accepted";
    SWlrBindCol out[1];
    int32_t     count = 0;
    ASSERT_EQ(wlrDecodeBind(buf, static_cast<uint32_t>(n), out, 1, &count), WLR_DECODE_OK);
    ASSERT_EQ(count, 1);
    ASSERT_NE(out[0].lengths, nullptr);
    EXPECT_EQ(wlrBindColLength(&out[0], 0), 3u);
    EXPECT_EQ(wlrBindColLength(&out[0], 1), 5u);
    EXPECT_EQ(out[0].valueBytes, 8u);
  }
}

TEST(wlrFormatConformance, BindSizeRejectsWrongFixedWidthExtent) {
  int32_t value = 7;
  SWlrBindEncodeCol col = {};
  col.type = TSDB_DATA_TYPE_INT;
  col.elemCount = 2;
  col.values = reinterpret_cast<const uint8_t*>(&value);
  col.valueBytes = sizeof(value);
  EXPECT_EQ(wlrBindEncodedSize(&col, 1), UINT32_MAX);
}

/*
 * Type codes must match taos.h, and an unknown code must be refused rather than
 * guessed at.
 *
 * Type 20 is GEOMETRY (variable-length), NOT a 16-byte DECIMAL128 -- DECIMAL is
 * 17. Treating 20 as fixed-width-16 silently encodes a GEOMETRY bind with no
 * length table, so replay reconstructs it with the wrong element width.
 */
TEST(wlrFormatConformance, BindTypeCodesMatchTaosHeader) {
  /*
   * Part 1: the numbers themselves. A stored type code is on-disk data, so the
   * literals are deliberate here even though the library now uses the macros --
   * this is what catches a macro being renumbered underneath the format, which
   * naming the constants on both sides could never detect.
   */
  EXPECT_EQ(TSDB_DATA_TYPE_BOOL, 1);
  EXPECT_EQ(TSDB_DATA_TYPE_TINYINT, 2);
  EXPECT_EQ(TSDB_DATA_TYPE_SMALLINT, 3);
  EXPECT_EQ(TSDB_DATA_TYPE_INT, 4);
  EXPECT_EQ(TSDB_DATA_TYPE_BIGINT, 5);
  EXPECT_EQ(TSDB_DATA_TYPE_FLOAT, 6);
  EXPECT_EQ(TSDB_DATA_TYPE_DOUBLE, 7);
  EXPECT_EQ(TSDB_DATA_TYPE_VARCHAR, 8);
  EXPECT_EQ(TSDB_DATA_TYPE_TIMESTAMP, 9);
  EXPECT_EQ(TSDB_DATA_TYPE_NCHAR, 10);
  EXPECT_EQ(TSDB_DATA_TYPE_UTINYINT, 11);
  EXPECT_EQ(TSDB_DATA_TYPE_USMALLINT, 12);
  EXPECT_EQ(TSDB_DATA_TYPE_UINT, 13);
  EXPECT_EQ(TSDB_DATA_TYPE_UBIGINT, 14);
  EXPECT_EQ(TSDB_DATA_TYPE_JSON, 15);
  EXPECT_EQ(TSDB_DATA_TYPE_VARBINARY, 16);
  EXPECT_EQ(TSDB_DATA_TYPE_DECIMAL, 17);
  EXPECT_EQ(TSDB_DATA_TYPE_BLOB, 18);
  EXPECT_EQ(TSDB_DATA_TYPE_MEDIUMBLOB, 19);
  EXPECT_EQ(TSDB_DATA_TYPE_GEOMETRY, 20) << "20 is GEOMETRY, never a DECIMAL128";
  EXPECT_EQ(TSDB_DATA_TYPE_DECIMAL64, 21);

  // Part 2: the library's classification, written in names so it states intent.
  EXPECT_EQ(wlrTypeFixedWidth(TSDB_DATA_TYPE_BOOL), 1u);
  EXPECT_EQ(wlrTypeFixedWidth(TSDB_DATA_TYPE_TINYINT), 1u);
  EXPECT_EQ(wlrTypeFixedWidth(TSDB_DATA_TYPE_SMALLINT), 2u);
  EXPECT_EQ(wlrTypeFixedWidth(TSDB_DATA_TYPE_INT), 4u);
  EXPECT_EQ(wlrTypeFixedWidth(TSDB_DATA_TYPE_BIGINT), 8u);
  EXPECT_EQ(wlrTypeFixedWidth(TSDB_DATA_TYPE_FLOAT), 4u);
  EXPECT_EQ(wlrTypeFixedWidth(TSDB_DATA_TYPE_DOUBLE), 8u);
  EXPECT_EQ(wlrTypeFixedWidth(TSDB_DATA_TYPE_TIMESTAMP), 8u);
  EXPECT_EQ(wlrTypeFixedWidth(TSDB_DATA_TYPE_DECIMAL64), 0u);

  // Variable-length types carry an explicit per-element length.
  EXPECT_TRUE(wlrTypeIsVarLen(TSDB_DATA_TYPE_VARCHAR));
  EXPECT_TRUE(wlrTypeIsVarLen(TSDB_DATA_TYPE_NCHAR));
  EXPECT_TRUE(wlrTypeIsVarLen(TSDB_DATA_TYPE_JSON));
  EXPECT_TRUE(wlrTypeIsVarLen(TSDB_DATA_TYPE_VARBINARY));
  EXPECT_TRUE(wlrTypeIsVarLen(TSDB_DATA_TYPE_DECIMAL));
  EXPECT_TRUE(wlrTypeIsVarLen(TSDB_DATA_TYPE_DECIMAL64));
  EXPECT_TRUE(wlrTypeIsVarLen(TSDB_DATA_TYPE_BLOB));
  EXPECT_TRUE(wlrTypeIsVarLen(TSDB_DATA_TYPE_MEDIUMBLOB));
  EXPECT_TRUE(wlrTypeIsVarLen(TSDB_DATA_TYPE_GEOMETRY))
      << "GEOMETRY is variable-length, not a fixed-width DECIMAL128";

  // Known-ness is separate from var-length-ness: an unrecognised code reports
  // var-length too, which is why the decoder cannot rely on width alone.
  EXPECT_TRUE(wlrTypeIsKnown(TSDB_DATA_TYPE_GEOMETRY));
  EXPECT_FALSE(wlrTypeIsKnown(TSDB_DATA_TYPE_MAX)) << "MAX is a bound, not a type";
  EXPECT_FALSE(wlrTypeIsKnown(0));
  EXPECT_FALSE(wlrTypeIsKnown(9999));
}

// An unknown type code must be rejected by the decoder rather than silently
// treated as variable-length, which would mis-parse the column layout.
TEST(wlrFormatConformance, BindDecodeRejectsUnknownType) {
  int32_t     v[2] = {7, 8};
  SWlrBindEncodeCol c = {};
  c.type = TSDB_DATA_TYPE_INT;
  c.elemCount = 2;
  c.values = reinterpret_cast<const uint8_t*>(v);
  c.valueBytes = sizeof(v);

  std::vector<uint8_t> buf(wlrBindEncodedSize(&c, 1));
  int32_t              n = wlrEncodeBind(buf.data(), buf.size(), &c, 1);
  ASSERT_GT(n, 0);

  SWlrBindCol out[1];
  int32_t     count = 0;
  ASSERT_EQ(wlrDecodeBind(buf.data(), static_cast<uint32_t>(n), out, 1, &count), WLR_DECODE_OK);

  // Rewrite the type code to one the format does not define.
  std::vector<uint8_t> bad(buf.begin(), buf.begin() + n);
  wlrPutU16(bad.data() + 4, 200);
  EXPECT_EQ(wlrDecodeBind(bad.data(), static_cast<uint32_t>(n), out, 1, &count),
            WLR_DECODE_BAD_LENGTH)
      << "an unknown type code must be refused, not assumed variable-length";
}

TEST(wlrFormatConformance, UntypedBindRequiresSingleNull) {
  uint8_t nullFlag = 1;
  SWlrBindEncodeCol column = {};
  column.type = 0;
  column.elemCount = 1;
  column.isNull = &nullFlag;
  uint32_t size = wlrBindEncodedSize(&column, 1);
  ASSERT_NE(size, UINT32_MAX);
  std::vector<uint8_t> encoded(size);
  ASSERT_GT(wlrEncodeBind(encoded.data(), size, &column, 1), 0);
  SWlrBindCol decoded = {};
  int32_t count = 0;
  ASSERT_EQ(wlrDecodeBind(encoded.data(), size, &decoded, 1, &count), WLR_DECODE_OK);
  EXPECT_EQ(decoded.type, 0);

  nullFlag = 0;
  EXPECT_EQ(wlrBindEncodedSize(&column, 1), UINT32_MAX);
  EXPECT_LT(wlrEncodeBind(encoded.data(), size, &column, 1), 0);
  encoded[size - 5] = 0;
  EXPECT_EQ(wlrDecodeBind(encoded.data(), size, &decoded, 1, &count), WLR_DECODE_BAD_LENGTH);
}

TEST(wlrFormatConformance, UntypedNoneRequiresSingleTristateElement) {
  uint8_t nullFlags[] = {2, 2};
  SWlrBindEncodeCol column = {};
  column.flags = WLR_BIND_FLAG_TRISTATE_NULL;
  column.elemCount = 1;
  column.isNull = nullFlags;
  uint32_t size = wlrBindEncodedSize(&column, 1);
  ASSERT_NE(size, UINT32_MAX);
  std::vector<uint8_t> encoded(size);
  ASSERT_EQ(wlrEncodeBind(encoded.data(), size, &column, 1), static_cast<int32_t>(size));
  SWlrBindCol decoded = {};
  int32_t count = 0;
  ASSERT_EQ(wlrDecodeBind(encoded.data(), size, &decoded, 1, &count), WLR_DECODE_OK);
  ASSERT_EQ(count, 1);
  EXPECT_EQ(decoded.type, 0);
  EXPECT_EQ(decoded.elemCount, 1);
  EXPECT_EQ(decoded.isNull[0], 2);
  EXPECT_EQ(decoded.valueBytes, 0u);

  column.flags = 0;
  EXPECT_EQ(wlrBindEncodedSize(&column, 1), UINT32_MAX);
  EXPECT_LT(wlrEncodeBind(encoded.data(), size, &column, 1), 0);
  wlrPutU16(encoded.data() + 6, 0);
  EXPECT_EQ(wlrDecodeBind(encoded.data(), size, &decoded, 1, &count), WLR_DECODE_BAD_LENGTH);
  column.flags = WLR_BIND_FLAG_TRISTATE_NULL;
  column.elemCount = 2;
  EXPECT_EQ(wlrBindEncodedSize(&column, 1), UINT32_MAX);
  EXPECT_LT(wlrEncodeBind(encoded.data(), size, &column, 1), 0);
  column.elemCount = 1;
  nullFlags[0] = 3;
  EXPECT_EQ(wlrBindEncodedSize(&column, 1), UINT32_MAX);
  EXPECT_LT(wlrEncodeBind(encoded.data(), size, &column, 1), 0);
  wlrPutU16(encoded.data() + 6, WLR_BIND_FLAG_TRISTATE_NULL);
  encoded[24] = 3;
  EXPECT_EQ(wlrDecodeBind(encoded.data(), size, &decoded, 1, &count), WLR_DECODE_BAD_LENGTH);
  nullFlags[0] = 2;
  column.type = 200;
  EXPECT_EQ(wlrBindEncodedSize(&column, 1), UINT32_MAX);
  EXPECT_LT(wlrEncodeBind(encoded.data(), size, &column, 1), 0);
}

TEST(wlrFormatConformance, Stmt2NamesPreserveNullAndEmptyEntries) {
  const char *names[] = {"t1", nullptr, "", "t2", nullptr};
  uint32_t size = wlrStmt2NamesEncodedSize(names, 5);
  ASSERT_EQ(size, 28u);
  std::vector<uint8_t> encoded(size);
  ASSERT_EQ(wlrEncodeStmt2Names(encoded.data(), size, names, 5), static_cast<int32_t>(size));
  SWlrSmlLine decoded[5] = {};
  int32_t count = 0;
  ASSERT_EQ(wlrDecodeStmt2Names(encoded.data(), size, decoded, 5, &count), WLR_DECODE_OK);
  ASSERT_EQ(count, 5);
  EXPECT_EQ(std::string(reinterpret_cast<const char *>(decoded[0].bytes), decoded[0].length), "t1");
  EXPECT_EQ(decoded[1].bytes, nullptr);
  EXPECT_EQ(decoded[1].length, 0u);
  EXPECT_NE(decoded[2].bytes, nullptr);
  EXPECT_EQ(decoded[2].length, 0u);
  EXPECT_EQ(std::string(reinterpret_cast<const char *>(decoded[3].bytes), decoded[3].length), "t2");
  EXPECT_EQ(decoded[4].bytes, nullptr);
  EXPECT_EQ(wlrDecodeStmt2Names(encoded.data(), size, nullptr, 0, &count), WLR_DECODE_OK);
  EXPECT_EQ(count, 5);
  EXPECT_EQ(wlrDecodeStmt2Names(encoded.data(), size, decoded, 4, &count), WLR_DECODE_NO_SPACE);
  EXPECT_EQ(wlrDecodeStmt2Names(encoded.data(), size - 1, decoded, 5, &count), WLR_DECODE_BAD_LENGTH);
  EXPECT_LT(wlrEncodeStmt2Names(encoded.data(), size - 1, names, 5), 0);
  EXPECT_EQ(wlrSmlLinesEncodedSize(names, 5), UINT32_MAX);
  EXPECT_LT(wlrEncodeSmlLines(encoded.data(), size, names, 5), 0);
  ASSERT_GT(wlrEncodeStmt2Names(encoded.data(), size, names, 5), 0);
  EXPECT_EQ(wlrDecodeSmlLines(encoded.data(), size, decoded, 5, &count), WLR_DECODE_BAD_LENGTH);

  const char *legacyNames[] = {"t1", ""};
  int32_t legacySize = wlrEncodeSmlLines(encoded.data(), size, legacyNames, 2);
  ASSERT_GT(legacySize, 0);
  ASSERT_EQ(wlrDecodeStmt2Names(encoded.data(), legacySize, decoded, 5, &count), WLR_DECODE_OK);
  EXPECT_EQ(count, 2);
  EXPECT_NE(decoded[1].bytes, nullptr);
  EXPECT_EQ(decoded[1].length, 0u);
}

TEST(wlrFormatConformance, Stmt2NamesPreserveAbsentNameArray) {
  uint8_t encoded[12] = {};
  EXPECT_EQ(wlrStmt2NamesEncodedSize(nullptr, 2), sizeof(encoded));
  ASSERT_EQ(wlrEncodeStmt2Names(encoded, sizeof(encoded), nullptr, 2), sizeof(encoded));
  SWlrSmlLine decoded[2] = {};
  int32_t count = 0;
  ASSERT_EQ(wlrDecodeStmt2Names(encoded, sizeof(encoded), decoded, 2, &count), WLR_DECODE_OK);
  EXPECT_EQ(count, 2);
  EXPECT_EQ(decoded[0].bytes, nullptr);
  EXPECT_EQ(decoded[1].bytes, nullptr);
  EXPECT_EQ(wlrSmlLinesEncodedSize(nullptr, 2), UINT32_MAX);
  EXPECT_LT(wlrEncodeSmlLines(encoded, sizeof(encoded), nullptr, 2), 0);
}

// A var-length column with no value bytes must still have lengths that agree.
// Skipping the check when valueBytes == 0 let a caller record a length table
// describing bytes the blob does not contain.
TEST(wlrFormatConformance, BindRejectsLengthsWithoutValueBytes) {
  int32_t lens[2] = {3, 5};
  uint8_t buf[256];

  // elemCount > 0, valueBytes == 0, but lengths claim 8 bytes: contradictory.
  {
    SWlrBindEncodeCol c = {};
    c.type = TSDB_DATA_TYPE_VARCHAR;
    c.elemCount = 2;
    c.lengths = lens;
    c.values = nullptr;
    c.valueBytes = 0;
    EXPECT_LT(wlrEncodeBind(buf, sizeof(buf), &c, 1), 0)
        << "lengths must not describe bytes the blob does not carry";
  }

  // An all-NULL column legitimately has no value bytes and needs no lengths.
  {
    uint8_t     nulls[2] = {1, 1};
    SWlrBindEncodeCol c = {};
    c.type = TSDB_DATA_TYPE_VARCHAR;
    c.elemCount = 2;
    c.isNull = nulls;
    c.lengths = nullptr;
    c.values = nullptr;
    c.valueBytes = 0;
    EXPECT_GT(wlrEncodeBind(buf, sizeof(buf), &c, 1), 0)
        << "an all-NULL var-length column is representable without lengths";
  }
}

// A NULL element contributes no value bytes, so its recorded length is ignored
// when checking that lengths delimit the values.
TEST(wlrFormatConformance, BindVarLenNullElementsExcludedFromExtent) {
  const char *vals = "abc";
  uint8_t     nulls[2] = {1, 0};
  int32_t     lens[2] = {99, 3};  // element 0 is NULL; its length must not count
  uint8_t     buf[256];

  SWlrBindEncodeCol c = {};
  c.type = TSDB_DATA_TYPE_VARCHAR;
  c.elemCount = 2;
  c.isNull = nulls;
  c.lengths = lens;
  c.values = reinterpret_cast<const uint8_t*>(vals);
  c.valueBytes = 3;

  int32_t n = wlrEncodeBind(buf, sizeof(buf), &c, 1);
  ASSERT_GT(n, 0) << "NULL elements must not be counted toward the value extent";

  SWlrBindCol out[1];
  int32_t     count = 0;
  ASSERT_EQ(wlrDecodeBind(buf, static_cast<uint32_t>(n), out, 1, &count), WLR_DECODE_OK);
  ASSERT_EQ(count, 1);
  EXPECT_EQ(out[0].isNull[0], 1);
  EXPECT_EQ(wlrBindColLength(&out[0], 1), 3u);
}

// TS ENC009 -- the encoding must contain no host pointers. A struct-copy encoder
// would embed buffer/length/is_null addresses; this checks they are absent.
TEST(wlrFormatConformance, BindEncodingContainsNoPointers) {
  int32_t     v[2] = {7, 8};
  uint8_t     nulls[2] = {0, 0};
  SWlrBindEncodeCol c = {};
  c.type = TSDB_DATA_TYPE_INT;
  c.elemCount = 2;
  c.isNull = nulls;
  c.values = reinterpret_cast<const uint8_t*>(v);
  c.valueBytes = sizeof(v);

  std::vector<uint8_t> buf(wlrBindEncodedSize(&c, 1));
  int32_t              n = wlrEncodeBind(buf.data(), buf.size(), &c, 1);
  ASSERT_GT(n, 0);

  // Each caller pointer, as raw bytes, must not appear anywhere in the payload.
  const void* ptrs[3] = {static_cast<const void*>(v), static_cast<const void*>(nulls),
                         static_cast<const void*>(&c)};
  for (const void* p : ptrs) {
    uintptr_t raw = reinterpret_cast<uintptr_t>(p);

    /* Both byte orders are searched. A leaked pointer would be written in host
     * order, and building only a little-endian needle would silently stop
     * detecting leaks on a big-endian host -- the guard would pass for the wrong
     * reason. Checking both keeps it meaningful on any platform. */
    uint8_t leNeedle[sizeof(uintptr_t)];
    uint8_t beNeedle[sizeof(uintptr_t)];
    for (size_t i = 0; i < sizeof(uintptr_t); ++i) {
      leNeedle[i] = static_cast<uint8_t>((raw >> (8 * i)) & 0xFF);
      beNeedle[sizeof(uintptr_t) - 1 - i] = leNeedle[i];
    }

    bool found = false;
    for (size_t i = 0; i + sizeof(uintptr_t) <= static_cast<size_t>(n); ++i) {
      if (std::memcmp(buf.data() + i, leNeedle, sizeof(uintptr_t)) == 0 ||
          std::memcmp(buf.data() + i, beNeedle, sizeof(uintptr_t)) == 0) {
        found = true;
        break;
      }
    }
    EXPECT_FALSE(found) << "a caller address leaked into the bind encoding";
  }
}

// Malformed bind blobs must be rejected, not misread.
TEST(wlrFormatConformance, BindDecodeRejectsMalformed) {
  int32_t     v[1] = {5};
  uint8_t     nulls[1] = {0};
  SWlrBindEncodeCol c = {};
  c.type = TSDB_DATA_TYPE_INT;
  c.elemCount = 1;
  c.isNull = nulls;
  c.values = reinterpret_cast<const uint8_t*>(v);
  c.valueBytes = sizeof(v);

  std::vector<uint8_t> good(wlrBindEncodedSize(&c, 1));
  int32_t              n = wlrEncodeBind(good.data(), good.size(), &c, 1);
  ASSERT_GT(n, 0);

  SWlrBindCol out[4];
  int32_t     count = 0;

  // Absurd column count.
  {
    std::vector<uint8_t> bad(good.begin(), good.begin() + n);
    wlrPutU32(bad.data(), 0xFFFFFF);
    EXPECT_EQ(wlrDecodeBind(bad.data(), static_cast<uint32_t>(bad.size()), out, 4, &count),
              WLR_DECODE_BAD_LENGTH);
  }
  // Fixed-width column claiming a length array.
  {
    std::vector<uint8_t> bad(good.begin(), good.begin() + n);
    wlrPutU32(bad.data() + 4 + 12, 4);  // lenBytes on a fixed type
    EXPECT_EQ(wlrDecodeBind(bad.data(), static_cast<uint32_t>(bad.size()), out, 4, &count),
              WLR_DECODE_BAD_LENGTH);
  }
  // Value bytes beyond the buffer.
  {
    std::vector<uint8_t> bad(good.begin(), good.begin() + n);
    wlrPutU32(bad.data() + 4 + 16, 0xFFFF);
    EXPECT_EQ(wlrDecodeBind(bad.data(), static_cast<uint32_t>(bad.size()), out, 4, &count),
              WLR_DECODE_BAD_LENGTH);
  }
  // Truncated buffer.
  EXPECT_EQ(wlrDecodeBind(good.data(), 6, out, 4, &count), WLR_DECODE_BAD_LENGTH);

  // A zero-element variable-width column cannot own unreachable value bytes.
  {
    std::vector<uint8_t> bad(4 + 20 + 1, 0);
    wlrPutU32(bad.data(), 1);
    wlrPutU16(bad.data() + 4, TSDB_DATA_TYPE_VARCHAR);
    wlrPutU32(bad.data() + 4 + 4, 0);  // zero elements cannot make the trailing value byte reachable
    wlrPutU32(bad.data() + 4 + 16, 1);
    EXPECT_EQ(wlrDecodeBind(bad.data(), static_cast<uint32_t>(bad.size()), out, 4, &count),
              WLR_DECODE_BAD_LENGTH);
  }
}

// DS 6.10 -- per-entry lengths and raw bytes, so line boundaries survive.
/*
 * The *EncodedSize helpers are the only bounds guard their encoders apply before
 * copying, so the size must never wrap. A uint32_t accumulator overflows once
 * count * entrySize passes 4 GiB, which would make the guard pass and the memcpy
 * run past the buffer. Saturating to UINT32_MAX instead keeps the guard honest,
 * and the encoders treat that value as "size unknown" and refuse.
 *
 * The huge counts here are never dereferenced: the size helpers stop at the
 * multiplication, and the encoders reject before touching any element.
 */
TEST(wlrFormatConformance, EncodedSizeSaturatesInsteadOfWrapping) {
  // count * 4 wraps a uint32_t here (0x40000001 * 4 == 4 mod 2^32).
  const int32_t wrapCount = 0x40000001;
  const char   *dummy[1] = {"x"};

  EXPECT_EQ(wlrSmlLinesEncodedSize(dummy, wrapCount), UINT32_MAX)
      << "a wrapped size would understate the buffer the encoder needs";

  uint8_t small[64];
  EXPECT_LT(wlrEncodeSmlLines(small, sizeof(small), dummy, wrapCount), 0)
      << "an unknown-size encode must be refused, not attempted";

  // Same for the connection-option list.
  SWlrConnOptIn opt[1];
  opt[0].type = 0;
  opt[0].name = "charset";
  opt[0].value = "UTF-8";
  EXPECT_EQ(wlrConnOptListEncodedSize(opt, wrapCount), UINT32_MAX);
  EXPECT_LT(wlrEncodeConnOptList(small, sizeof(small), opt, wrapCount), 0);

  // A negative elemCount must not produce a plausible-looking size either.
  SWlrBindEncodeCol bad = {};
  bad.type = TSDB_DATA_TYPE_VARCHAR;
  bad.elemCount = -1;
  EXPECT_EQ(wlrBindEncodedSize(&bad, 1), UINT32_MAX);
  bad.elemCount = 1;
  bad.valueBytes = 1;
  bad.values = nullptr;
  EXPECT_EQ(wlrBindEncodedSize(&bad, 1), UINT32_MAX);

  EXPECT_EQ(wlrBindEncodedSize(nullptr, 1), UINT32_MAX);
  EXPECT_EQ(wlrSmlLinesEncodedSize(nullptr, 1), UINT32_MAX);
  EXPECT_EQ(wlrConnOptListEncodedSize(nullptr, 1), UINT32_MAX);
  EXPECT_EQ(wlrBindEncodedSize(nullptr, -1), UINT32_MAX);
  EXPECT_EQ(wlrSmlLinesEncodedSize(nullptr, -1), UINT32_MAX);
  EXPECT_EQ(wlrConnOptListEncodedSize(nullptr, -1), UINT32_MAX);
  opt[0].name = "";
  EXPECT_EQ(wlrConnOptListEncodedSize(opt, 1), UINT32_MAX);
  EXPECT_LT(wlrEncodeConnOptList(small, sizeof(small), opt, 1), 0);
}

TEST(wlrFormatConformance, SmlLinesRejectNullEntry) {
  const char *lines[] = {"", nullptr};
  uint8_t buf[32] = {};
  EXPECT_EQ(wlrSmlLinesEncodedSize(lines, 2), UINT32_MAX);
  EXPECT_LT(wlrEncodeSmlLines(buf, sizeof(buf), lines, 2), 0);
}

TEST(wlrFormatConformance, EmptyListsEncodeConsistently) {
  uint8_t buf[4] = {0xff, 0xff, 0xff, 0xff};
  int32_t count = -1;

  ASSERT_EQ(wlrEncodeBind(buf, sizeof(buf), nullptr, 0), 4);
  EXPECT_EQ(wlrDecodeBind(buf, sizeof(buf), nullptr, 0, &count), WLR_DECODE_OK);
  EXPECT_EQ(count, 0);

  ASSERT_EQ(wlrEncodeSmlLines(buf, sizeof(buf), nullptr, 0), 4);
  EXPECT_EQ(wlrDecodeSmlLines(buf, sizeof(buf), nullptr, 0, &count), WLR_DECODE_OK);
  EXPECT_EQ(count, 0);

  ASSERT_EQ(wlrEncodeConnOptList(buf, sizeof(buf), nullptr, 0), 4);
  EXPECT_EQ(wlrDecodeConnOptList(buf, sizeof(buf), nullptr, 0, &count), WLR_DECODE_OK);
  EXPECT_EQ(count, 0);
}

TEST(wlrFormatConformance, SmlLinesRoundTrip) {
  const char *lines[3] = {
      "meters,loc=a cur=1.0 1700000000000",
      "",                                            // empty line is still an entry
      "meters,loc=b note=\"has,comma and \\n\" 1700000001000",
  };
  uint32_t need = wlrSmlLinesEncodedSize(lines, 3);
  std::vector<uint8_t> buf(need);
  int32_t n = wlrEncodeSmlLines(buf.data(), buf.size(), lines, 3);
  ASSERT_GT(n, 0);

  SWlrSmlLine out[8];
  int32_t     count = 0;
  ASSERT_EQ(wlrDecodeSmlLines(buf.data(), static_cast<uint32_t>(n), out, 8, &count), WLR_DECODE_OK);
  ASSERT_EQ(count, 3);
  for (int i = 0; i < 3; ++i) {
    ASSERT_EQ(out[i].length, strlen(lines[i])) << "length mismatch at entry " << i;
    if (out[i].length > 0) {
      EXPECT_EQ(std::memcmp(out[i].bytes, lines[i], out[i].length), 0) << "bytes differ at " << i;
    }
  }
}

TEST(wlrFormatConformance, SmlLinesBoundaryCount) {
  const char *line = "meters,loc=a cur=1 1700000000000";
  for (int32_t expected : {512, 513}) {
    std::vector<const char *> lines(expected, line);
    std::vector<uint8_t> buf(wlrSmlLinesEncodedSize(lines.data(), expected));
    int32_t encoded = wlrEncodeSmlLines(buf.data(), buf.size(), lines.data(), expected);
    ASSERT_GT(encoded, 0);

    int32_t count = 0;
    ASSERT_EQ(wlrDecodeSmlLines(buf.data(), static_cast<uint32_t>(encoded), nullptr, 0, &count),
              WLR_DECODE_OK);
    ASSERT_EQ(count, expected);
    std::vector<SWlrSmlLine> decoded(count);
    ASSERT_EQ(wlrDecodeSmlLines(buf.data(), static_cast<uint32_t>(encoded), decoded.data(), count, &count),
              WLR_DECODE_OK);
    ASSERT_EQ(count, expected);
    for (const SWlrSmlLine &entry : decoded) {
      EXPECT_EQ(entry.length, strlen(line));
      EXPECT_EQ(std::memcmp(entry.bytes, line, entry.length), 0);
    }
  }
}

// A line containing an embedded newline must not be split: lengths delimit entries,
// not separators.
TEST(wlrFormatConformance, SmlLineWithEmbeddedNewlineSurvives) {
  const char *lines[1] = {"meters,loc=x note=\"line1\nline2\" 1700000000000"};
  ASSERT_NE(strchr(lines[0], '\n'), nullptr);

  std::vector<uint8_t> buf(wlrSmlLinesEncodedSize(lines, 1));
  int32_t n = wlrEncodeSmlLines(buf.data(), buf.size(), lines, 1);
  ASSERT_GT(n, 0);

  SWlrSmlLine out[1];
  int32_t     count = 0;
  ASSERT_EQ(wlrDecodeSmlLines(buf.data(), static_cast<uint32_t>(n), out, 1, &count), WLR_DECODE_OK);
  ASSERT_EQ(count, 1);
  EXPECT_EQ(out[0].length, strlen(lines[0]));
  EXPECT_EQ(std::memcmp(out[0].bytes, lines[0], out[0].length), 0);
}

TEST(wlrFormatConformance, SmlLinesDecodeRejectsMalformed) {
  const char *lines[1] = {"abc"};
  std::vector<uint8_t> good(wlrSmlLinesEncodedSize(lines, 1));
  int32_t n = wlrEncodeSmlLines(good.data(), good.size(), lines, 1);
  ASSERT_GT(n, 0);

  SWlrSmlLine out[4];
  int32_t     count = 0;
  EXPECT_EQ(wlrDecodeSmlLines(good.data(), static_cast<uint32_t>(n), out, 0, &count),
            WLR_DECODE_NO_SPACE);
  EXPECT_EQ(wlrDecodeSmlLines(good.data(), static_cast<uint32_t>(n), nullptr, 1, &count),
            WLR_DECODE_BAD_ARG);
  EXPECT_EQ(wlrDecodeSmlLines(good.data(), static_cast<uint32_t>(n), out, -1, &count),
            WLR_DECODE_BAD_ARG);

  // Absurd entry count.
  {
    std::vector<uint8_t> bad(good.begin(), good.begin() + n);
    wlrPutU32(bad.data(), 0xFFFFFF);
    EXPECT_EQ(wlrDecodeSmlLines(bad.data(), static_cast<uint32_t>(n), out, 4, &count),
              WLR_DECODE_BAD_LENGTH);
  }
  // Entry length beyond the buffer.
  {
    std::vector<uint8_t> bad(good.begin(), good.begin() + n);
    wlrPutU32(bad.data() + 4, 0xFFFF);
    EXPECT_EQ(wlrDecodeSmlLines(bad.data(), static_cast<uint32_t>(n), out, 4, &count),
              WLR_DECODE_BAD_LENGTH);
  }
  // Truncated header.
  EXPECT_EQ(wlrDecodeSmlLines(good.data(), 2, out, 4, &count), WLR_DECODE_BAD_LENGTH);
  // Trailing bytes not described by the length table.
  good.push_back(0);
  EXPECT_EQ(wlrDecodeSmlLines(good.data(), static_cast<uint32_t>(n) + 1, out, 4, &count),
            WLR_DECODE_BAD_LENGTH);
}

#endif  // WLR_CONFORMANCE

#ifdef WLR_HAVE_REAL_HEADER

// The reference model above and wlrFormat.h are two statements of the same spec.
// If they disagree, one of them is wrong -- fail rather than let the tests keep
// validating a model the shipped header contradicts.
TEST(wlrFormat, WlrHeaderMatchesReference) {
  ASSERT_EQ(std::strlen(WLR_FORMAT_MAGIC), static_cast<size_t>(WLR_FORMAT_MAGIC_LEN));
  EXPECT_EQ(std::memcmp(WLR_FORMAT_MAGIC, kMagic, WLR_FORMAT_MAGIC_LEN), 0);
  EXPECT_EQ(WLR_FORMAT_MAJOR, kMajor);
  EXPECT_EQ(WLR_FORMAT_MINOR, kMinor);
  EXPECT_EQ(WLR_FORMAT_ENDIAN_MARK, kEndianMark);
  EXPECT_EQ(WLR_FRAME_DELIMITER, kDelimiter);
  EXPECT_EQ(WLR_MAX_FRAME_PAYLOAD, kMaxFramePayload);
  EXPECT_EQ(WLR_CHUNK_SIZE, kChunkSize);

  // Frame field offsets the reference encoder hard-codes.
  EXPECT_EQ(WLR_FRAME_OFF_LENGTH, 0);
  EXPECT_EQ(WLR_FRAME_OFF_TYPE, 4);
  EXPECT_EQ(WLR_FRAME_OFF_FLAGS, 6);
  EXPECT_EQ(WLR_FRAME_OFF_EVENT_SEQ, 8);
  EXPECT_EQ(WLR_FRAME_OFF_LOGICAL_ID, 16);
  EXPECT_EQ(WLR_FRAME_OFF_PAYLOAD_LEN, 24);
  EXPECT_EQ(WLR_FRAME_OFF_PAYLOAD, 28);
  EXPECT_EQ(WLR_FRAME_CRC_PREFIX_LEN, 24);
  EXPECT_EQ(WLR_FRAME_CRC_SIZE, 4);
  EXPECT_EQ(WLR_FRAME_MIN_LENGTH, 24 + WLR_FRAME_CRC_SIZE + 1);
  EXPECT_EQ(WLR_TLV_OFF_TAG, 0);
  EXPECT_EQ(WLR_TLV_OFF_FLAGS, 2);
  EXPECT_EQ(WLR_TLV_OFF_LENGTH, 4);
  EXPECT_EQ(WLR_TLV_OFF_VALUE, 8);
  EXPECT_EQ(WLR_TLV_HEADER_SIZE, 8);

  // Header layout, including the trailing size the reference patches in.
  EXPECT_EQ(WLR_HDR_OFF_MAGIC, 0);
  EXPECT_EQ(WLR_HDR_OFF_MAJOR, 8);
  EXPECT_EQ(WLR_HDR_OFF_MINOR, 10);
  EXPECT_EQ(WLR_HDR_OFF_ENDIAN, 12);
  EXPECT_EQ(WLR_HDR_OFF_HDRLEN, 14);
  EXPECT_EQ(WLR_HDR_OFF_SESSION_ID, 16);
  EXPECT_EQ(WLR_SESSION_ID_LEN, 16);

  FileHeader h{};
  std::vector<uint8_t> encoded = encodeHeader(h);
  EXPECT_EQ(encoded.size(), static_cast<size_t>(WLR_FILE_HEADER_SIZE))
      << "reference header size disagrees with WLR_FILE_HEADER_SIZE";

  // TLV tag numbers.
  //
  // Cast to a common integer type on purpose: each pair compares the shipped header's
  // enum against the reference model's separate enum, which is exactly what this test
  // exists to check. Comparing the enum types directly trips -Werror=enum-compare in
  // the CI build.
  EXPECT_EQ(WLR_TAG_REQUIRED_BIT, kRequiredBit);
#define WLR_EXPECT_HEADER_TAG(referenceName, productionName, value) \
  EXPECT_EQ((uint32_t)productionName, (uint32_t)referenceName);
  WLR_TEST_TAGS(WLR_EXPECT_HEADER_TAG)
#undef WLR_EXPECT_HEADER_TAG

  // Frame types.
  EXPECT_EQ((uint32_t)WLR_FRAME_EVENT, (uint32_t)FRAME_EVENT);
  EXPECT_EQ((uint32_t)WLR_FRAME_CHUNK_BEGIN, (uint32_t)FRAME_CHUNK_BEGIN);
  EXPECT_EQ((uint32_t)WLR_FRAME_CHUNK_DATA, (uint32_t)FRAME_CHUNK_DATA);
  EXPECT_EQ((uint32_t)WLR_FRAME_CHUNK_END, (uint32_t)FRAME_CHUNK_END);
  EXPECT_EQ((uint32_t)WLR_FRAME_SESSION_STOP, (uint32_t)FRAME_SESSION_STOP);
  EXPECT_EQ((uint32_t)WLR_FRAME_FATAL, (uint32_t)FRAME_FATAL);
}

// Version gate must live in the library, not be re-derived per caller.
TEST(wlrFormat, HeaderVersionGateConstants) {
#ifdef WLR_CONFORMANCE
  EXPECT_NE(WLR_FORMAT_MAJOR, 2) << "major 2 is the rejected case in MajorVersionGate";
  EXPECT_EQ(WLR_MAX_LOGICAL_PAYLOAD, 64ull << 30);
  EXPECT_EQ(wlrChunkCount(0), 0u);
  EXPECT_EQ(wlrChunkCount(WLR_MAX_LOGICAL_PAYLOAD),
            (uint32_t)((WLR_MAX_LOGICAL_PAYLOAD + WLR_CHUNK_SIZE - 1) / WLR_CHUNK_SIZE));
  EXPECT_EQ(wlrChunkCount(WLR_CHUNK_SIZE + 1), 2u);
  EXPECT_EQ(wlrChunkCount(WLR_MAX_LOGICAL_PAYLOAD + 1), UINT32_MAX);
  EXPECT_TRUE(wlrNeedsChunking(WLR_MAX_FRAME_PAYLOAD + 1ull));
  EXPECT_TRUE(wlrNeedsChunking(WLR_MAX_LOGICAL_PAYLOAD));
  EXPECT_FALSE(wlrNeedsChunking(WLR_MAX_LOGICAL_PAYLOAD + 1ull));
  EXPECT_EQ(WLR_SEGMENT_ROTATE_SIZE, 1ull << 30);
  EXPECT_EQ(WLR_CFG_BUFFER_MB_MIN, 16);
  EXPECT_EQ(WLR_CFG_BUFFER_MB_MAX, 4096);
  EXPECT_EQ(WLR_CFG_BUFFER_MB_DEFAULT, 256);
  EXPECT_EQ(WLR_DIR_PERM, 0700);
  EXPECT_EQ(WLR_FILE_PERM, 0600);
#else
  GTEST_SKIP() << "the production format implementation is unavailable";
#endif
}

#endif  // WLR_HAVE_REAL_HEADER

#ifdef WLR_CONFORMANCE

// The library and the reference model must produce identical bytes. This is the
// assertion that makes the reference worth keeping: it turns "the spec says X"
// into a build-time check on the shipped encoder.
TEST(wlrFormatConformance, EncoderMatchesReferenceByteForByte) {
  taosResolveCRC();

  const std::string sql = "select * from t1 where c1 > 3\nand c2 < 9";
  std::vector<Tlv>  refTlvs{
      tlvU64(TAG_WALL_NS, 1753800000000000000ull),
      tlvU64(TAG_MONO_NS, 987654321ull),
      tlvU64(TAG_ORIGIN_TID, 7ull),
      tlvU64(TAG_CALL_ID, 99ull),
      tlvStr(TAG_SQL, sql),
  };
  Frame                refFrame = makeEventFrame(4242, refTlvs);
  std::vector<uint8_t> refBytes = encodeFrame(refFrame);

  // Same event through the real library.
  std::vector<uint8_t> payload(4096);
  size_t               off = 0;
  int32_t              n;
  n = wlrEncodeTlvU64(payload.data() + off, payload.size() - off, WLR_TAG_WALL_NS, true,
                      1753800000000000000ull);
  ASSERT_GT(n, 0);
  off += static_cast<size_t>(n);
  n = wlrEncodeTlvU64(payload.data() + off, payload.size() - off, WLR_TAG_MONO_NS, true, 987654321ull);
  ASSERT_GT(n, 0);
  off += static_cast<size_t>(n);
  n = wlrEncodeTlvU64(payload.data() + off, payload.size() - off, WLR_TAG_ORIGIN_TID, true, 7ull);
  ASSERT_GT(n, 0);
  off += static_cast<size_t>(n);
  n = wlrEncodeTlvU64(payload.data() + off, payload.size() - off, WLR_TAG_CALL_ID, true, 99ull);
  ASSERT_GT(n, 0);
  off += static_cast<size_t>(n);
  n = wlrEncodeTlv(payload.data() + off, payload.size() - off, WLR_TAG_SQL, true, 0, sql.data(),
                   static_cast<uint32_t>(sql.size()));
  ASSERT_GT(n, 0);
  off += static_cast<size_t>(n);

  SWlrFrameHeader hdr;
  std::memset(&hdr, 0, sizeof(hdr));
  hdr.type = WLR_FRAME_EVENT;
  hdr.flags = 0;
  hdr.eventSequence = 4242;
  hdr.logicalEventId = 4242;
  hdr.payloadLength = static_cast<uint32_t>(off);

  std::vector<uint8_t> libBytes(wlrFrameEncodedSize(hdr.payloadLength));
  int32_t              written = wlrEncodeFrame(libBytes.data(), libBytes.size(), &hdr, payload.data());
  ASSERT_GT(written, 0);
  libBytes.resize(static_cast<size_t>(written));

  ASSERT_EQ(libBytes.size(), refBytes.size()) << "encoded frame size differs from reference";
  EXPECT_EQ(std::memcmp(libBytes.data(), refBytes.data(), refBytes.size()), 0)
      << "library and DS 6.15.2 reference disagree on frame bytes";

  /* Exercise the CRC path at empty, sub-block, exact-block, and cross-block
   * payload sizes. The reference hashes the body in one pass; these cases keep
   * the production implementation's streaming CRC semantics honest at boundaries. */
  const size_t payloadLengths[] = {0, 1, 63, 64, 65, 255, 256, 257, 4095, 4096, 4097};
  for (size_t payloadLen : payloadLengths) {
    Frame boundaryFrame = {};
    boundaryFrame.type = WLR_FRAME_EVENT;
    boundaryFrame.eventSeq = 5000 + payloadLen;
    boundaryFrame.logicalEventId = 6000 + payloadLen;
    boundaryFrame.payload.resize(payloadLen);
    for (size_t i = 0; i < payloadLen; ++i) {
      boundaryFrame.payload[i] = static_cast<uint8_t>((i * 131u + payloadLen) & 0xffu);
    }
    std::vector<uint8_t> boundaryRef = encodeFrame(boundaryFrame);

    SWlrFrameHeader boundaryHdr = {};
    boundaryHdr.type = boundaryFrame.type;
    boundaryHdr.eventSequence = boundaryFrame.eventSeq;
    boundaryHdr.logicalEventId = boundaryFrame.logicalEventId;
    boundaryHdr.payloadLength = static_cast<uint32_t>(payloadLen);
    std::vector<uint8_t> boundaryLib(wlrFrameEncodedSize(boundaryHdr.payloadLength));
    const uint8_t *boundaryPayload = payloadLen == 0 ? nullptr : boundaryFrame.payload.data();
    int32_t boundaryWritten =
        wlrEncodeFrame(boundaryLib.data(), boundaryLib.size(), &boundaryHdr, boundaryPayload);
    ASSERT_GT(boundaryWritten, 0) << "payload length " << payloadLen;
    boundaryLib.resize(static_cast<size_t>(boundaryWritten));
    ASSERT_EQ(boundaryLib.size(), boundaryRef.size()) << "payload length " << payloadLen;
    EXPECT_EQ(std::memcmp(boundaryLib.data(), boundaryRef.data(), boundaryRef.size()), 0)
        << "CRC/frame mismatch at payload length " << payloadLen;
  }
}

// The library decoder must classify each damage mode the same way the reference
// does -- in particular it must keep torn tails distinct from real corruption.
TEST(wlrFormatConformance, DecoderClassifiesDamageLikeReference) {
  taosResolveCRC();

  Frame                f = makeEventFrame(9, {tlvU64(TAG_MONO_NS, 9)});
  std::vector<uint8_t> good = encodeFrame(f);

  SWlrFrameHeader hdr;
  const uint8_t*  payload = nullptr;
  uint32_t        consumed = 0;

  // Clean frame.
  EXPECT_EQ(wlrDecodeFrame(good.data(), good.size(), &hdr, &payload, &consumed), WLR_DECODE_OK);
  EXPECT_EQ(consumed, good.size());
  EXPECT_EQ(hdr.eventSequence, 9u);

  // Truncated: torn tail, NOT corruption.
  EXPECT_EQ(wlrDecodeFrame(good.data(), good.size() - 5, &hdr, &payload, &consumed),
            WLR_DECODE_TORN_TAIL);

  // Payload bit flipped: CRC mismatch.
  {
    std::vector<uint8_t> bad = good;
    bad[bad.size() - 6] ^= 0xFF;
    EXPECT_EQ(wlrDecodeFrame(bad.data(), bad.size(), &hdr, &payload, &consumed),
              WLR_DECODE_CRC_MISMATCH);
  }
  // Delimiter clobbered but CRC intact: unterminated frame.
  {
    std::vector<uint8_t> bad = good;
    bad.back() = 0x00;
    hdr.type = 0xaaaa;
    payload = reinterpret_cast<const uint8_t*>(static_cast<uintptr_t>(1));
    consumed = 0xdeadbeef;
    EXPECT_EQ(wlrDecodeFrame(bad.data(), bad.size(), &hdr, &payload, &consumed),
              WLR_DECODE_MISSING_LF);
    EXPECT_EQ(hdr.type, 0xaaaau);
    EXPECT_EQ(payload, reinterpret_cast<const uint8_t*>(static_cast<uintptr_t>(1)));
    EXPECT_EQ(consumed, 0xdeadbeefu);
  }
  // Hostile payload length.
  {
    std::vector<uint8_t> bad = good;
    uint32_t             huge = WLR_MAX_FRAME_PAYLOAD + 1;
    for (int i = 0; i < 4; ++i) {
      bad[WLR_FRAME_OFF_PAYLOAD_LEN + i] = static_cast<uint8_t>((huge >> (8 * i)) & 0xFF);
    }
    EXPECT_EQ(wlrDecodeFrame(bad.data(), bad.size(), &hdr, &payload, &consumed),
              WLR_DECODE_BAD_LENGTH);
  }
}

// TLV walk: unknown optional tags are returned to the caller to skip, and a
// hostile length must not walk off the payload.
TEST(wlrFormatConformance, TlvDecodeBoundsAreEnforced) {
  std::vector<uint8_t> payload(512);
  int32_t n = wlrEncodeTlvU64(payload.data(), payload.size(), WLR_TAG_MONO_NS, true, 5);
  ASSERT_GT(n, 0);

  SWlrTlv tlvs[8];
  int32_t count = 0;
  EXPECT_EQ(wlrDecodeTlvs(payload.data(), static_cast<uint32_t>(n), tlvs, 8, &count), WLR_DECODE_OK);
  EXPECT_EQ(count, 1);
  EXPECT_EQ(WLR_TAG_BASE(tlvs[0].tag), WLR_TAG_MONO_NS);
  EXPECT_TRUE(WLR_TAG_IS_REQUIRED(tlvs[0].tag));

  int32_t countOnly = 0;
  EXPECT_EQ(wlrDecodeTlvs(payload.data(), static_cast<uint32_t>(n), nullptr, 0, &countOnly),
            WLR_DECODE_OK);
  EXPECT_EQ(countOnly, 1);
  EXPECT_EQ(wlrDecodeTlvs(payload.data(), static_cast<uint32_t>(n), nullptr, 1, &countOnly),
            WLR_DECODE_BAD_ARG);
  EXPECT_EQ(wlrDecodeTlvs(payload.data(), static_cast<uint32_t>(n), tlvs, -1, &countOnly),
            WLR_DECODE_BAD_ARG);

  std::vector<uint8_t> twoTlvs(64);
  int32_t first = wlrEncodeTlvU64(twoTlvs.data(), twoTlvs.size(), WLR_TAG_MONO_NS, true, 1);
  ASSERT_GT(first, 0);
  int32_t second = wlrEncodeTlvU64(twoTlvs.data() + first, twoTlvs.size() - first,
                                   WLR_TAG_CALL_ID, true, 2);
  ASSERT_GT(second, 0);
  tlvs[0].tag = 0xBEEF;
  countOnly = 0;
  EXPECT_EQ(wlrDecodeTlvs(twoTlvs.data(), static_cast<uint32_t>(first + second), tlvs, 1,
                          &countOnly),
            WLR_DECODE_NO_SPACE);
  EXPECT_EQ(countOnly, 2);
  EXPECT_EQ(tlvs[0].tag, 0xBEEF) << "NO_SPACE must leave the caller's array unchanged";

  uint8_t emptyValue[WLR_TLV_HEADER_SIZE];
  int32_t emptyLen = wlrEncodeTlv(emptyValue, sizeof(emptyValue), WLR_TAG_DB, false, 0, nullptr, 0);
  ASSERT_EQ(emptyLen, WLR_TLV_HEADER_SIZE);
  EXPECT_EQ(wlrDecodeTlvs(emptyValue, static_cast<uint32_t>(emptyLen), nullptr, 0, &countOnly),
            WLR_DECODE_OK);
  EXPECT_EQ(countOnly, 1);

  // Overstate the value length: must be rejected, not read past the end.
  uint32_t lie = 0xFFFFFF00u;
  for (int i = 0; i < 4; ++i) {
    payload[4 + i] = static_cast<uint8_t>((lie >> (8 * i)) & 0xFF);
  }
  EXPECT_EQ(wlrDecodeTlvs(payload.data(), static_cast<uint32_t>(n), tlvs, 8, &count),
            WLR_DECODE_BAD_LENGTH);
}

TEST(wlrFormatConformance, TlvDecodeAcceptsExactMaximumCount) {
  const int32_t maxTlvs = WLR_MAX_FRAME_PAYLOAD / WLR_TLV_HEADER_SIZE;
  std::vector<uint8_t> payload(WLR_MAX_FRAME_PAYLOAD, 0);
  std::vector<SWlrTlv> tlvs(static_cast<size_t>(maxTlvs));
  int32_t count = 0;

  EXPECT_EQ(wlrDecodeTlvs(payload.data(), static_cast<uint32_t>(payload.size()), nullptr, 0, &count),
            WLR_DECODE_OK);
  ASSERT_EQ(count, maxTlvs);
  int32_t decoded = 0;
  EXPECT_EQ(wlrDecodeTlvs(payload.data(), static_cast<uint32_t>(payload.size()), tlvs.data(), maxTlvs,
                          &decoded),
            WLR_DECODE_OK);
  EXPECT_EQ(decoded, maxTlvs);
}

// File header round-trip through the library, including the version gate.
TEST(wlrFormatConformance, FileHeaderClockDomainCompatibility) {
  SWlrFileHeader header{};
  std::strcpy(header.clockDomainId, "linux:boot-a:0:0");
  std::vector<uint8_t> bytes(WLR_FILE_HEADER_SIZE);
  ASSERT_EQ(wlrEncodeFileHeader(bytes.data(), bytes.size(), &header), WLR_FILE_HEADER_SIZE);
  SWlrFileHeader decoded{};
  EXPECT_EQ(wlrDecodeFileHeader(bytes.data(), WLR_FILE_HEADER_V0_SIZE, &decoded), WLR_DECODE_BAD_LENGTH);

  std::vector<uint8_t> legacy(bytes.begin(), bytes.begin() + WLR_FILE_HEADER_V0_SIZE);
  wlrPutU16(legacy.data() + WLR_HDR_OFF_MINOR, 0);
  wlrPutU16(legacy.data() + WLR_HDR_OFF_HDRLEN, WLR_FILE_HEADER_V0_SIZE);
  ASSERT_EQ(wlrDecodeFileHeader(legacy.data(), legacy.size(), &decoded), WLR_DECODE_OK);
  EXPECT_STREQ(decoded.clockDomainId, "");
  EXPECT_EQ(decoded.headerLength, WLR_FILE_HEADER_V0_SIZE);

  memset(bytes.data() + WLR_HDR_OFF_CLOCK_DOMAIN, 'x', WLR_CLOCK_DOMAIN_ID_LEN);
  EXPECT_EQ(wlrDecodeFileHeader(bytes.data(), bytes.size(), &decoded), WLR_DECODE_BAD_LENGTH);
  memset(header.clockDomainId, 'x', sizeof(header.clockDomainId));
  EXPECT_LT(wlrEncodeFileHeader(bytes.data(), bytes.size(), &header), 0);
}

TEST(wlrFormatConformance, FileHeaderRoundTrip) {
  SWlrFileHeader in;
  std::memset(&in, 0, sizeof(in));
  std::memset(in.sessionId, 0x5A, sizeof(in.sessionId));
  in.processStartWallNs = 1753800000000000000ll;
  in.processStartMonoNs = 12345678ll;
  in.pid = 90210;
  in.segmentIndex = 7;
  in.firstEventSeq = 1000;
  std::strcpy(in.clockDomainId, "linux:00000000-0000-0000-0000-000000000001:0:0");

  std::vector<uint8_t> buf(WLR_FILE_HEADER_SIZE);
  ASSERT_EQ(wlrEncodeFileHeader(buf.data(), buf.size(), &in), WLR_FILE_HEADER_SIZE);

  // Must match the reference encoder byte for byte.
  FileHeader refHdr{};
  std::memset(refHdr.sessionId, 0x5A, sizeof(refHdr.sessionId));
  refHdr.processStartWallNs = in.processStartWallNs;
  refHdr.processStartMonoNs = in.processStartMonoNs;
  refHdr.pid = in.pid;
  refHdr.segmentIndex = in.segmentIndex;
  refHdr.firstEventSeq = in.firstEventSeq;
  std::strcpy(refHdr.clockDomainId, in.clockDomainId);
  std::vector<uint8_t> refBuf = encodeHeader(refHdr);
  ASSERT_EQ(buf.size(), refBuf.size());
  EXPECT_EQ(std::memcmp(buf.data(), refBuf.data(), buf.size()), 0);

  SWlrFileHeader out;
  ASSERT_EQ(wlrDecodeFileHeader(buf.data(), buf.size(), &out), WLR_DECODE_OK);
  EXPECT_EQ(out.major, WLR_FORMAT_MAJOR);
  EXPECT_EQ(out.minor, WLR_FORMAT_MINOR);
  EXPECT_EQ(out.headerLength, WLR_FILE_HEADER_SIZE);
  EXPECT_EQ(out.pid, in.pid);
  EXPECT_EQ(out.segmentIndex, in.segmentIndex);
  EXPECT_EQ(out.firstEventSeq, in.firstEventSeq);
  /* Decoded from their own offsets, so a wrong one would go unnoticed without these:
   * both are replay-critical, since the wall/mono pair anchors every event's time. */
  EXPECT_EQ(out.processStartWallNs, in.processStartWallNs);
  EXPECT_EQ(out.processStartMonoNs, in.processStartMonoNs);
  EXPECT_EQ(std::memcmp(out.sessionId, in.sessionId, WLR_SESSION_ID_LEN), 0);
  EXPECT_STREQ(out.clockDomainId, in.clockDomainId);

  EXPECT_EQ(wlrDecodeFileHeader(nullptr, buf.size(), &out), WLR_DECODE_BAD_ARG);
  EXPECT_EQ(wlrDecodeFileHeader(buf.data(), buf.size(), nullptr), WLR_DECODE_BAD_ARG);

  // A declared extension without its bytes is truncated; with the byte present it
  // is a forward-compatible minor-version header that readers skip.
  std::vector<uint8_t> oversizedHeader = buf;
  oversizedHeader[WLR_HDR_OFF_HDRLEN] = static_cast<uint8_t>((WLR_FILE_HEADER_SIZE + 1) & 0xFF);
  oversizedHeader[WLR_HDR_OFF_HDRLEN + 1] =
      static_cast<uint8_t>(((WLR_FILE_HEADER_SIZE + 1) >> 8) & 0xFF);
  EXPECT_EQ(wlrDecodeFileHeader(oversizedHeader.data(), oversizedHeader.size(), &out),
            WLR_DECODE_BAD_LENGTH);
  oversizedHeader.push_back(0);
  EXPECT_EQ(wlrDecodeFileHeader(oversizedHeader.data(), oversizedHeader.size(), &out),
            WLR_DECODE_OK);
  EXPECT_EQ(out.headerLength, WLR_FILE_HEADER_SIZE + 1);

  // Bad magic and unsupported major must be refused.
  {
    std::vector<uint8_t> bad = buf;
    bad[0] = 'X';
    EXPECT_EQ(wlrDecodeFileHeader(bad.data(), bad.size(), &out), WLR_DECODE_BAD_MAGIC);
  }
  {
    std::vector<uint8_t> bad = buf;
    bad[WLR_HDR_OFF_MAJOR] = 2;
    EXPECT_EQ(wlrDecodeFileHeader(bad.data(), bad.size(), &out), WLR_DECODE_BAD_VERSION);
  }
  // A host-endian writer would emit the mark byte-swapped; reject, never swap.
  {
    std::vector<uint8_t> bad = buf;
    bad[WLR_HDR_OFF_ENDIAN] = 0x01;
    bad[WLR_HDR_OFF_ENDIAN + 1] = 0x02;
    EXPECT_EQ(wlrDecodeFileHeader(bad.data(), bad.size(), &out), WLR_DECODE_BAD_ENDIAN);
  }
}

// TS API007 -- the required-tag table is enforced, and it is the only copy.
TEST(wlrFormatConformance, RequiredTagTableEnforced) {
  std::vector<uint8_t> payload(4096);
  size_t               off = 0;
  auto                 addU64 = [&](uint16_t tag, uint64_t v) {
    int32_t n = wlrEncodeTlvU64(payload.data() + off, payload.size() - off, tag, true, v);
    ASSERT_GT(n, 0);
    off += static_cast<size_t>(n);
  };

  // A query BEGIN missing its SQL tag must be rejected.
  addU64(WLR_TAG_WALL_NS, 1);
  addU64(WLR_TAG_MONO_NS, 2);
  addU64(WLR_TAG_ORIGIN_TID, 3);
  addU64(WLR_TAG_API_TYPE, WLR_API_QUERY);
  addU64(WLR_TAG_EVENT_PHASE, WLR_PHASE_BEGIN);
  addU64(WLR_TAG_CALL_ID, 4);
  addU64(WLR_TAG_CONN_ID, 5);

  SWlrTlv tlvs[32];
  int32_t count = 0;
  ASSERT_EQ(wlrDecodeTlvs(payload.data(), static_cast<uint32_t>(off), tlvs, 32, &count),
            WLR_DECODE_OK);
  EXPECT_EQ(wlrValidateRequiredTags(WLR_FRAME_EVENT, WLR_API_QUERY, WLR_PHASE_BEGIN, tlvs, count),
            WLR_DECODE_UNKNOWN_REQUIRED_TAG)
      << "query BEGIN without SQL must fail validation";

  // Adding SQL satisfies the rule.
  const char* sql = "select 1";
  int32_t     n = wlrEncodeTlv(payload.data() + off, payload.size() - off, WLR_TAG_SQL, true, 0, sql,
                               static_cast<uint32_t>(strlen(sql)));
  ASSERT_GT(n, 0);
  off += static_cast<size_t>(n);
  ASSERT_EQ(wlrDecodeTlvs(payload.data(), static_cast<uint32_t>(off), tlvs, 32, &count),
            WLR_DECODE_OK);
  EXPECT_EQ(wlrValidateRequiredTags(WLR_FRAME_EVENT, WLR_API_QUERY, WLR_PHASE_BEGIN, tlvs, count),
            WLR_DECODE_OK);

  // Every recorded API must be covered by some rule, so a new API cannot be
  // encoded without a schema decision.
  int32_t coveredApis = 0;
  for (uint16_t api = 1; api < WLR_API_MAX; ++api) {
    if (wlrGetRequiredTags(WLR_FRAME_EVENT, api, WLR_PHASE_BEGIN) != nullptr) ++coveredApis;
    const bool defined = (api <= WLR_API_CONNECT_TOKEN) ||
                         (api >= WLR_API_QUERY && api <= WLR_API_VALIDATE_SQL) ||
                         (api >= WLR_API_FETCH_ROW && api <= WLR_API_FREE_RESULT) ||
                         (api >= WLR_API_STMT_INIT && api <= WLR_API_STMT_CLOSE) ||
                         (api >= WLR_API_STMT2_INIT && api <= WLR_API_STMT2_CLOSE) ||
                         (api >= WLR_API_SML_INSERT && api <= WLR_API_SML_INSERT_TTL_WITH_REQID_TBNAME_KEY);
    if (!defined) continue;
    const SWlrRequiredTags* r = wlrGetRequiredTags(WLR_FRAME_EVENT, api, WLR_PHASE_BEGIN);
    EXPECT_NE(r, nullptr) << "no required-tag rule for API type " << api;
  }
  constexpr int32_t kAdditionalDiagnosticApis = 3;
  static_assert(WLR_API_CONNECT_TOKEN - WLR_API_CONNECT_AUTH + 1 == kAdditionalDiagnosticApis,
                "diagnostic API count must track the auth API enum range");
  EXPECT_EQ(coveredApis, WLR_RECORDED_API_COUNT + kAdditionalDiagnosticApis)
      << "the schema allowlist and recorded API enum/count are out of sync";
}

TEST(wlrFormatConformance, SessionStopRequiresActiveCallCount) {
  std::vector<uint8_t> payload(256);
  size_t               off = 0;
  auto                 addU64 = [&](uint16_t tag, uint64_t value) {
    int32_t n = wlrEncodeTlvU64(payload.data() + off, payload.size() - off, tag, false, value);
    ASSERT_GT(n, 0);
    off += static_cast<size_t>(n);
  };

  addU64(WLR_TAG_WALL_NS, 1);
  addU64(WLR_TAG_MONO_NS, 2);
  addU64(WLR_TAG_SML_COUNT, 1);

  SWlrTlv tlvs[8];
  int32_t count = 0;
  ASSERT_EQ(wlrDecodeTlvs(payload.data(), static_cast<uint32_t>(off), tlvs, 8, &count), WLR_DECODE_OK);
  EXPECT_EQ(wlrValidateRequiredTags(WLR_FRAME_SESSION_STOP, WLR_API_UNKNOWN, 0, tlvs, count),
            WLR_DECODE_UNKNOWN_REQUIRED_TAG);

  addU64(WLR_TAG_ACTIVE_CALL_COUNT, 1);
  addU64(WLR_TAG_CALL_ID, 42);
  ASSERT_EQ(wlrDecodeTlvs(payload.data(), static_cast<uint32_t>(off), tlvs, 8, &count), WLR_DECODE_OK);
  EXPECT_EQ(wlrValidateRequiredTags(WLR_FRAME_SESSION_STOP, WLR_API_UNKNOWN, 0, tlvs, count),
            WLR_DECODE_OK);

  payload.resize(off - 16);
  ASSERT_EQ(wlrDecodeTlvs(payload.data(), static_cast<uint32_t>(payload.size()), tlvs, 8, &count),
            WLR_DECODE_OK);
  EXPECT_EQ(wlrValidateRequiredTags(WLR_FRAME_SESSION_STOP, WLR_API_UNKNOWN, 0, tlvs, count),
            WLR_DECODE_BAD_LENGTH);
}

// A single-column bind records which column it targeted. -1 means "all columns"
// in the row-mode APIs, so the tag has to survive as a signed value: reading it
// back as a huge unsigned would turn all-columns into column 4294967295.
TEST(wlrFormatConformance, BindColIdxSurvivesNegativeSentinel) {
  std::vector<uint8_t> buf(64);

  int32_t n = wlrEncodeTlvU64(buf.data(), buf.size(), WLR_TAG_BIND_COL_IDX, false,
                              static_cast<uint64_t>(static_cast<int64_t>(-1)));
  ASSERT_GT(n, 0);

  SWlrTlv tlvs[4];
  int32_t count = 0;
  ASSERT_EQ(wlrDecodeTlvs(buf.data(), static_cast<uint32_t>(n), tlvs, 4, &count), WLR_DECODE_OK);
  ASSERT_EQ(count, 1);
  EXPECT_EQ(WLR_TAG_BASE(tlvs[0].tag), WLR_TAG_BIND_COL_IDX);
  EXPECT_FALSE(WLR_TAG_IS_REQUIRED(tlvs[0].tag))
      << "BIND_COL_IDX must stay optional so 1.0/1.1 readers skip it by length";

  ASSERT_EQ(tlvs[0].length, 8u);
  EXPECT_EQ(static_cast<int32_t>(static_cast<int64_t>(wlrGetU64(tlvs[0].value))), -1);

  // A real column index round-trips as itself, including 0 -- which is why the
  // recorder carries a separate "set" flag rather than treating 0 as absent.
  n = wlrEncodeTlvU64(buf.data(), buf.size(), WLR_TAG_BIND_COL_IDX, false, 0);
  ASSERT_GT(n, 0);
  ASSERT_EQ(wlrDecodeTlvs(buf.data(), static_cast<uint32_t>(n), tlvs, 4, &count), WLR_DECODE_OK);
  EXPECT_EQ(static_cast<int32_t>(static_cast<int64_t>(wlrGetU64(tlvs[0].value))), 0);
}

// taos_connect_with applies a whole option bag as one call, so the list has to
// survive a round trip with each option's fields still associated.
TEST(wlrFormatConformance, ConnOptListRoundTrip) {
  SWlrConnOptIn in[3];
  in[0].type = 0;
  in[0].name = "charset";
  in[0].value = "UTF-8";
  // Cleared: a legal call, and distinct from the empty string below.
  in[1].type = 1;
  in[1].name = "timezone";
  in[1].value = nullptr;
  in[2].type = 3;
  in[2].name = "userApp";
  in[2].value = "";

  std::vector<uint8_t> buf(wlrConnOptListEncodedSize(in, 3));
  int32_t              n = wlrEncodeConnOptList(buf.data(), buf.size(), in, 3);
  ASSERT_GT(n, 0);

  SWlrConnOpt out[3];
  int32_t     count = 0;
  ASSERT_EQ(wlrDecodeConnOptList(buf.data(), static_cast<uint32_t>(n), out, 3, &count),
            WLR_DECODE_OK);
  ASSERT_EQ(count, 3);

  EXPECT_EQ(out[0].type, 0);
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(out[0].name), out[0].nameLen), "charset");
  EXPECT_TRUE(out[0].hasValue);
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(out[0].value), out[0].valueLen), "UTF-8");

  // The whole point of the hasValue flag: cleared and "" must not decode alike.
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(out[1].name), out[1].nameLen), "timezone");
  EXPECT_FALSE(out[1].hasValue);
  EXPECT_EQ(out[1].valueLen, 0u);

  EXPECT_EQ(std::string(reinterpret_cast<const char*>(out[2].name), out[2].nameLen), "userApp");
  EXPECT_TRUE(out[2].hasValue);
  EXPECT_EQ(out[2].valueLen, 0u);
}

// A truncated or lying length table must be rejected, not read past.
TEST(wlrFormatConformance, ConnOptListDecodeRejectsMalformed) {
  SWlrConnOptIn in[1];
  in[0].type = 0;
  in[0].name = "charset";
  in[0].value = "UTF-8";

  std::vector<uint8_t> buf(wlrConnOptListEncodedSize(in, 1));
  int32_t              n = wlrEncodeConnOptList(buf.data(), buf.size(), in, 1);
  ASSERT_GT(n, 0);

  SWlrConnOpt out[1];
  int32_t     count = 0;

  // Each case below is a length/capacity fault, so the exact code is pinned: a
  // regression to some other non-OK value is itself a behaviour change worth
  // failing on.
  //
  // Header alone, no entry table.
  EXPECT_EQ(wlrDecodeConnOptList(buf.data(), 4, out, 1, &count), WLR_DECODE_BAD_LENGTH);
  // Truncated mid-payload.
  EXPECT_EQ(wlrDecodeConnOptList(buf.data(), static_cast<uint32_t>(n) - 2, out, 1, &count),
            WLR_DECODE_BAD_LENGTH);
  // A count far beyond what the buffer can hold must not be trusted.
  std::vector<uint8_t> bad(buf.begin(), buf.end());
  wlrPutU32(bad.data(), 0xFFFFFFFFu);
  EXPECT_EQ(wlrDecodeConnOptList(bad.data(), static_cast<uint32_t>(n), out, 1, &count),
            WLR_DECODE_BAD_LENGTH);
  // hasValue is a wire boolean, not an arbitrary non-zero integer.
  bad.assign(buf.begin(), buf.end());
  wlrPutU32(bad.data() + 4 + 12, 2);
  EXPECT_EQ(wlrDecodeConnOptList(bad.data(), static_cast<uint32_t>(n), out, 1, &count),
            WLR_DECODE_BAD_LENGTH);
  // A cleared option cannot simultaneously describe value bytes.
  bad.assign(buf.begin(), buf.end());
  wlrPutU32(bad.data() + 4 + 12, 0);
  EXPECT_EQ(wlrDecodeConnOptList(bad.data(), static_cast<uint32_t>(n), out, 1, &count),
            WLR_DECODE_BAD_LENGTH);
  // Capacity is respected rather than overrun.
  EXPECT_EQ(wlrDecodeConnOptList(buf.data(), static_cast<uint32_t>(n), out, 0, &count),
            WLR_DECODE_NO_SPACE);
  EXPECT_EQ(wlrDecodeConnOptList(buf.data(), static_cast<uint32_t>(n), nullptr, 1, &count),
            WLR_DECODE_BAD_ARG);
  EXPECT_EQ(wlrDecodeConnOptList(buf.data(), static_cast<uint32_t>(n), out, -1, &count),
            WLR_DECODE_BAD_ARG);
  // The decoder must consume the exact payload rather than ignore an
  // under-described trailing region.
  std::vector<uint8_t> trailing(buf);
  trailing.push_back(0xA5);
  EXPECT_EQ(wlrDecodeConnOptList(trailing.data(), static_cast<uint32_t>(trailing.size()), out, 1,
                                &count),
            WLR_DECODE_BAD_LENGTH);

  // Encoder and decoder enforce the same option-type range, so an invalid value
  // is rejected while recording rather than producing an unreplayable payload.
  in[0].type = WLR_CONN_OPT_TYPE_MAX + 1;
  EXPECT_EQ(wlrEncodeConnOptList(buf.data(), buf.size(), in, 1), -1);
  in[0].type = WLR_CONN_OPT_TYPE_MIN - 1;
  EXPECT_EQ(wlrEncodeConnOptList(buf.data(), buf.size(), in, 1), -1);
}

// A connection-option event must say WHICH option it set. Recording only that some
// option changed is not replayable: charset and timezone alter how later statements
// are interpreted, so replay has to reapply the same one.
TEST(wlrFormatConformance, ConnOptSchemaRequiresOptionIdentity) {
  std::vector<uint8_t> payload(1024);
  size_t               off = 0;
  auto                 addU64 = [&](uint16_t tag, uint64_t v) {
    int32_t n = wlrEncodeTlvU64(payload.data() + off, payload.size() - off, tag, true, v);
    ASSERT_GT(n, 0);
    off += static_cast<size_t>(n);
  };

  addU64(WLR_TAG_WALL_NS, 1);
  addU64(WLR_TAG_MONO_NS, 2);
  addU64(WLR_TAG_ORIGIN_TID, 3);
  addU64(WLR_TAG_API_TYPE, WLR_API_OPTIONS_CONNECTION);
  addU64(WLR_TAG_EVENT_PHASE, WLR_PHASE_BEGIN);
  addU64(WLR_TAG_CALL_ID, 4);

  SWlrTlv tlvs[32];
  int32_t count = 0;
  ASSERT_EQ(wlrDecodeTlvs(payload.data(), static_cast<uint32_t>(off), tlvs, 32, &count),
            WLR_DECODE_OK);
  EXPECT_EQ(wlrValidateRequiredTags(WLR_FRAME_EVENT, WLR_API_OPTIONS_CONNECTION, WLR_PHASE_BEGIN,
                                    tlvs, count),
            WLR_DECODE_UNKNOWN_REQUIRED_TAG)
      << "conn-option BEGIN without the option identity must fail validation";

  addU64(WLR_TAG_CONN_OPT_TYPE, 1);
  const char* name = "charset";
  int32_t     n = wlrEncodeTlv(payload.data() + off, payload.size() - off, WLR_TAG_CONN_OPT_NAME,
                               true, 0, name, static_cast<uint32_t>(strlen(name)));
  ASSERT_GT(n, 0);
  off += static_cast<size_t>(n);

  ASSERT_EQ(wlrDecodeTlvs(payload.data(), static_cast<uint32_t>(off), tlvs, 32, &count),
            WLR_DECODE_OK);
  EXPECT_EQ(wlrValidateRequiredTags(WLR_FRAME_EVENT, WLR_API_OPTIONS_CONNECTION, WLR_PHASE_BEGIN,
                                    tlvs, count),
            WLR_DECODE_OK);

  // The VALUE tag stays optional on purpose: taos_options_connection(taos, opt, NULL)
  // clears the option, and an absent tag is how the file says "cleared". Requiring it
  // would force the recorder to write an empty string, a different fact.
  const SWlrRequiredTags* r =
      wlrGetRequiredTags(WLR_FRAME_EVENT, WLR_API_OPTIONS_CONNECTION, WLR_PHASE_BEGIN);
  ASSERT_NE(r, nullptr);
  for (int32_t i = 0; i < r->tagCount; ++i) {
    EXPECT_NE(r->tags[i], WLR_TAG_CONN_OPT_VALUE)
        << "CONN_OPT_VALUE must stay optional so a cleared option is representable";
  }
}

/*
 * The rule table is scanned first-match, and its wildcard rows (apiType
 * WLR_API_UNKNOWN, phase 0) match anything. Correctness therefore depends on
 * specific rows preceding wildcard ones, which nothing in the lookup enforces --
 * reordering the table would silently return the wrong required set.
 *
 * This pins the observable consequence rather than the ordering itself: each API
 * below has a schema stricter than the common fallback, so if a wildcard row ever
 * shadows it the returned set loses those tags and this fails.
 */
TEST(wlrFormatConformance, SpecificRulesAreNotShadowedByWildcards) {
  struct Expect {
    uint16_t api;
    uint16_t phase;
    uint16_t mustRequire;
    const char* what;
  };
  // Each pair is a tag the common fallback does NOT contain, so its presence
  // proves the specific row won the lookup.
  const Expect cases[] = {
      // Not ENDPOINT: taos_connect(NULL, ...) is legal, so an absent endpoint
      // means "unspecified" and the tag is deliberately optional.
      {WLR_API_CONNECT, WLR_PHASE_BEGIN, WLR_TAG_USER, "connect BEGIN"},
      {WLR_API_QUERY, WLR_PHASE_BEGIN, WLR_TAG_SQL, "query BEGIN"},
      {WLR_API_QUERY, WLR_PHASE_RETURN, WLR_TAG_RET_CODE, "query RETURN"},
      {WLR_API_SELECT_DB, WLR_PHASE_BEGIN, WLR_TAG_DB, "select-db BEGIN"},
      {WLR_API_FETCH_ROW, WLR_PHASE_BEGIN, WLR_TAG_RESULT_ID, "fetch BEGIN"},
      {WLR_API_OPTIONS_CONNECTION, WLR_PHASE_BEGIN, WLR_TAG_CONN_OPT_NAME, "conn-opt BEGIN"},
      {WLR_API_OPTIONS_CONNECTION, WLR_PHASE_RETURN, WLR_TAG_RET_CODE, "conn-opt RETURN"},
      {WLR_API_SML_INSERT, WLR_PHASE_BEGIN, WLR_TAG_SML_PROTOCOL, "schemaless BEGIN"},
      {WLR_API_STMT_EXECUTE, WLR_PHASE_RETURN, WLR_TAG_RET_CODE, "statement RETURN"},
      {WLR_API_STMT2_BIND_PARAM_A, WLR_PHASE_DONE, WLR_TAG_RET_CODE, "statement DONE"},
      {WLR_API_STMT2_BIND_PARAM_A, WLR_PHASE_DONE, WLR_TAG_STMT_ID, "statement DONE scope"},
  };

  for (const auto& c : cases) {
    const SWlrRequiredTags* r = wlrGetRequiredTags(WLR_FRAME_EVENT, c.api, c.phase);
    ASSERT_NE(r, nullptr) << c.what << " has no rule at all";
    bool found = false;
    for (int32_t i = 0; i < r->tagCount; ++i) {
      if (r->tags[i] == c.mustRequire) {
        found = true;
        break;
      }
    }
    EXPECT_TRUE(found) << c.what << " resolved to a rule missing tag " << c.mustRequire
                       << " -- a wildcard row is shadowing the specific one";
  }
}

/*
 * Structural version of the check above.
 *
 * The test before this one enumerates specific (api, phase) pairs by hand, so it only
 * catches shadowing of the rules someone remembered to list. This asks the table itself:
 * wlrGetRequiredTags returns the FIRST match, so correctness rests on every wildcard row
 * (apiType == WLR_API_UNKNOWN, or phase == 0) sitting after every specific row it could
 * match. wlrFindShadowedRule walks all pairs and reports the first violation, so a
 * misplaced wildcard fails here regardless of which API it happens to affect.
 */
TEST(wlrFormatConformance, NoWildcardRuleShadowsASpecificOne) {
  EXPECT_EQ(wlrFindShadowedRule(), -1)
      << "a wildcard row in kRules precedes a more specific row, silently replacing that "
         "row's schema for every lookup that should have reached it";
}

/*
 * Forward compatibility, both directions (DS 6.15.2). A required tag this build does
 * not know must be rejected -- otherwise a file from a newer minor that made some new
 * tag mandatory would replay with that instruction silently missing. An unknown
 * OPTIONAL tag must be accepted, which is what makes minor additions safe.
 */
TEST(wlrFormatConformance, UnknownRequiredTagIsRejected) {
  // A query BEGIN with everything its schema requires.
  SWlrTlv tlvs[8] = {};
  tlvs[0].tag = WLR_TAG_WALL_NS;
  tlvs[1].tag = WLR_TAG_MONO_NS;
  tlvs[2].tag = WLR_TAG_ORIGIN_TID;
  tlvs[3].tag = WLR_TAG_API_TYPE;
  tlvs[4].tag = WLR_TAG_EVENT_PHASE;
  tlvs[5].tag = WLR_TAG_CALL_ID;
  tlvs[6].tag = WLR_TAG_CONN_ID;
  tlvs[7].tag = WLR_TAG_SQL;
  ASSERT_EQ(wlrValidateRequiredTags(WLR_FRAME_EVENT, WLR_API_QUERY, WLR_PHASE_BEGIN, tlvs, 8),
            WLR_DECODE_OK);

  // Tag 999 does not exist in this build. Optional: skipped, frame still valid.
  SWlrTlv withOptional[9] = {};
  std::memcpy(withOptional, tlvs, sizeof(tlvs));
  withOptional[8].tag = 999;
  EXPECT_EQ(wlrValidateRequiredTags(WLR_FRAME_EVENT, WLR_API_QUERY, WLR_PHASE_BEGIN, withOptional, 9),
            WLR_DECODE_OK)
      << "an unknown optional tag must be skipped, not rejected";

  // Same tag marked required: the frame carries an instruction this build cannot
  // honour, so replaying it would silently drop that meaning.
  withOptional[8].tag = (uint16_t)(999 | WLR_TAG_REQUIRED_BIT);
  EXPECT_EQ(wlrValidateRequiredTags(WLR_FRAME_EVENT, WLR_API_QUERY, WLR_PHASE_BEGIN, withOptional, 9),
            WLR_DECODE_UNKNOWN_REQUIRED_TAG)
      << "an unknown REQUIRED tag must be rejected (wlrFormat.h forward-compat rule)";

  // A known tag marked required but not in this API's schema is fine: the required
  // bit is the writer's encoding choice, not a claim about the schema.
  withOptional[8].tag = (uint16_t)(WLR_TAG_DURATION_NS | WLR_TAG_REQUIRED_BIT);
  EXPECT_EQ(wlrValidateRequiredTags(WLR_FRAME_EVENT, WLR_API_QUERY, WLR_PHASE_BEGIN, withOptional, 9),
            WLR_DECODE_OK)
      << "a known tag must not be rejected merely because this API does not require it";
}

TEST(wlrFormatConformance, UnknownFrameTypeIsRejected) {
  EXPECT_EQ(wlrValidateRequiredTags(UINT16_MAX, WLR_API_UNKNOWN, 0, nullptr, 0),
            WLR_DECODE_UNKNOWN_REQUIRED_TAG)
      << "a frame type unknown to this build must not pass schema validation";
}

TEST(wlrFormatConformance, RequiredTagValidationBoundsCallerCount) {
  SWlrTlv tlv = {};
  EXPECT_EQ(wlrValidateRequiredTags(WLR_FRAME_EVENT, WLR_API_QUERY, WLR_PHASE_BEGIN, &tlv,
                                    static_cast<int32_t>(WLR_MAX_FRAME_PAYLOAD / WLR_TLV_HEADER_SIZE + 1)),
            WLR_DECODE_BAD_LENGTH);
  EXPECT_EQ(wlrValidateRequiredTags(WLR_FRAME_EVENT, WLR_API_QUERY, WLR_PHASE_BEGIN, nullptr, 1),
            WLR_DECODE_BAD_ARG);
  EXPECT_EQ(wlrValidateRequiredTags(WLR_FRAME_EVENT, WLR_API_QUERY, WLR_PHASE_BEGIN, &tlv, -1),
            WLR_DECODE_BAD_ARG);
}

TEST(wlrFormatConformance, DuplicateTagsAreRejected) {
  SWlrTlv tlvs[9] = {};
  tlvs[0].tag = WLR_TAG_WALL_NS;
  tlvs[1].tag = WLR_TAG_MONO_NS;
  tlvs[2].tag = WLR_TAG_ORIGIN_TID;
  tlvs[3].tag = WLR_TAG_API_TYPE;
  tlvs[4].tag = WLR_TAG_EVENT_PHASE;
  tlvs[5].tag = WLR_TAG_CALL_ID;
  tlvs[6].tag = WLR_TAG_CONN_ID;
  tlvs[7].tag = WLR_TAG_SQL;
  tlvs[8].tag = WLR_TAG_SQL;
  EXPECT_EQ(wlrValidateRequiredTags(WLR_FRAME_EVENT, WLR_API_QUERY, WLR_PHASE_BEGIN, tlvs, 9),
            WLR_DECODE_BAD_LENGTH);
}

TEST(wlrFormatConformance, DuplicateTagsAreRejectedAfterInlineSetSpills) {
  std::vector<SWlrTlv> tlvs(8 + 65 + 1);
  tlvs[0].tag = WLR_TAG_WALL_NS;
  tlvs[1].tag = WLR_TAG_MONO_NS;
  tlvs[2].tag = WLR_TAG_ORIGIN_TID;
  tlvs[3].tag = WLR_TAG_API_TYPE;
  tlvs[4].tag = WLR_TAG_EVENT_PHASE;
  tlvs[5].tag = WLR_TAG_CALL_ID;
  tlvs[6].tag = WLR_TAG_CONN_ID;
  tlvs[7].tag = WLR_TAG_SQL;
  for (size_t i = 0; i < 65; ++i) tlvs[8 + i].tag = static_cast<uint16_t>(1000 + i);
  tlvs.back().tag = tlvs[8 + 64].tag;
  EXPECT_EQ(wlrValidateRequiredTags(WLR_FRAME_EVENT, WLR_API_QUERY, WLR_PHASE_BEGIN,
                                    tlvs.data(), static_cast<int32_t>(tlvs.size())),
            WLR_DECODE_BAD_LENGTH);
}

// Every tag the enum defines must be known to wlrTagIsKnown, or a frame the recorder
// itself wrote with that tag required would be rejected by its own validator.
TEST(wlrFormatConformance, EveryDefinedTagIsKnown) {
  for (uint16_t t : knownTags()) {
    EXPECT_TRUE(wlrTagIsKnown(t)) << "tag " << t << " is defined but wlrTagIsKnown says otherwise";
    // The required bit must not change the answer.
    EXPECT_TRUE(wlrTagIsKnown((uint16_t)(t | WLR_TAG_REQUIRED_BIT))) << "tag " << t;
  }
  EXPECT_FALSE(wlrTagIsKnown(999));
  EXPECT_FALSE(wlrTagIsKnown(0));
}

// Connect events must never require or carry a credential tag.
TEST(wlrFormatConformance, ConnectSchemaHasNoSecretTag) {
  const SWlrRequiredTags* r = wlrGetRequiredTags(WLR_FRAME_EVENT, WLR_API_CONNECT, WLR_PHASE_BEGIN);
  ASSERT_NE(r, nullptr);
  for (int32_t i = 0; i < r->tagCount; ++i) {
    // There is deliberately no password/token/TOTP tag in EWlrTag at all; this
    // guards against one being added and wired into the connect schema.
    EXPECT_NE(r->tags[i], 105) << "tag 105 must not become a credential field";
    EXPECT_NE(r->tags[i], 106) << "tag 106 must not become a credential field";
  }
}

#endif  // WLR_CONFORMANCE

#ifndef WLR_CONFORMANCE
TEST(wlrFormat, ConformanceSuiteMustBeCompiledIn) {
  /*
   * A hard failure, not a warning or a skip.
   *
   * This used to SUCCEED() while the implementation was still absent, which was
   * right then and wrong now: source/libs/wlr/src has landed, this target links
   * `wlr`, and that library exports WLR_FORMAT_HAVE_IMPL as a PUBLIC compile
   * definition. So reaching this test means the conformance gate broke -- the
   * PUBLIC definition was dropped, the link was changed, or wlrFormat.h stopped
   * being found -- and every wlrFormatConformance case silently compiled out.
   *
   * That is exactly the failure mode worth being loud about: 26 tests of the
   * shipped encoder disappearing while CI still reports green. Failing here means
   * the gate itself is tested.
   */
  FAIL() << "wlrFormatConformance compiled out: expected WLR_HAVE_REAL_HEADER and "
            "WLR_FORMAT_HAVE_IMPL (exported PUBLIC by the wlr library) to be defined. "
            "The shipped encoder/decoder is NOT being tested in this build.";
}
#endif
