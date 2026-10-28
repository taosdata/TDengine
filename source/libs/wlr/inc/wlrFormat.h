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
 * Workload Recorder (WLR) on-disk format.
 *
 * This header is the single shared definition of the recording file format. It
 * is consumed by two independent binaries:
 *
 *   - the Recorder embedded in libtaos (source/client/src/clientWlr*.c)
 *   - the standalone taosreplay tool (tools/taosReplay/)
 *
 * The format is the ONLY data protocol between them (DS 7.2). This library must
 * not depend on client internals (DS 4.2).
 *
 * Invariants that must not be broken:
 *
 *  1. All on-disk integers are little-endian and fixed width, on every
 *     platform. The structs below are IN-MEMORY representations only -- they are
 *     never memcpy'd to or from disk, because struct padding and host byte order
 *     are not part of the format. Always go through the encode/decode helpers.
 *  2. No raw pointers and no platform ABI structs are ever serialized (DS 8,
 *     DS 6.9: serializing TAOS_MULTI_BIND / TAOS_STMT2_BIND memory is forbidden).
 *  3. Strings carry an explicit length and are NOT NUL-terminated.
 *  4. Every frame ends with LF (0x0A) so a binary file still shows record
 *     boundaries in a pager. LF is a convenience for humans ONLY: parsers split
 *     by frameLength/payloadLength and never by newline, because SQL and binary
 *     payloads legitimately contain 0x0A (DS 6.15.2, FS 4.9).
 *  5. Major version mismatch is a hard reject. A minor bump may only add
 *     optional tags or new frame types (DS 11.3).
 *  6. The required-tag table (wlrRequiredTags) is the single source of truth,
 *     shared by recorder and parser. Hand-maintaining a second copy is
 *     forbidden (DS 6.15.2).
 *
 * Must compile under GCC 7 for the community/core image (DS 4.3).
 */

#ifndef TDENGINE_WLR_FORMAT_H_
#define TDENGINE_WLR_FORMAT_H_

#include "os.h"
#include "tdef.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------------------------------------------------ version */

#define WLR_FORMAT_MAGIC     "TDWREC01"
#define WLR_FORMAT_MAGIC_LEN 8

/* Bump major only for an incompatible change; parsers reject unknown majors. */
#define WLR_FORMAT_MAJOR 1
/* Version 1.1 appends the monotonic clock-domain identity to segment headers.
 * Readers accept future minors of the same major; incompatible changes bump MAJOR. */
#define WLR_FORMAT_MINOR 1

/* Written as a u16; a reader seeing 0x0201 is looking at a foreign-endian file
 * produced by a non-conforming writer and must reject it rather than swap. */
#define WLR_FORMAT_ENDIAN_MARK 0x0102

#define WLR_SESSION_ID_LEN 16
#define WLR_CLOCK_DOMAIN_ID_LEN 80

/* --------------------------------------------------------------- geometry */

/*
 * Segment file header (DS 6.15.2):
 *
 *   magic[8] | major:u16 | minor:u16 | endian:u16 | headerLength:u16 |
 *   sessionId[16] | processStartWallNs:i64 | processStartMonoNs:i64 |
 *   pid:u64 | segmentIndex:u32 | firstEventSeq:u64 | clockDomainId[80]
 *
 * headerLength is stored so a future minor version may append fields; readers
 * skip to headerLength rather than assuming this exact size.
 */
#define WLR_HDR_OFF_MAGIC        0
#define WLR_HDR_OFF_MAJOR        8
#define WLR_HDR_OFF_MINOR        10
#define WLR_HDR_OFF_ENDIAN       12
#define WLR_HDR_OFF_HDRLEN       14
#define WLR_HDR_OFF_SESSION_ID   16
#define WLR_HDR_OFF_START_WALL   32
#define WLR_HDR_OFF_START_MONO   40
#define WLR_HDR_OFF_PID          48
#define WLR_HDR_OFF_SEGMENT_IDX  56
#define WLR_HDR_OFF_FIRST_SEQ    60
#define WLR_FILE_HEADER_V0_SIZE  68
#define WLR_HDR_OFF_CLOCK_DOMAIN 68
#define WLR_FILE_HEADER_SIZE    (WLR_HDR_OFF_CLOCK_DOMAIN + WLR_CLOCK_DOMAIN_ID_LEN)

/*
 * Frame (DS 6.15.2):
 *
 *   frameLength:u32 | frameType:u16 | flags:u16 | eventSequence:u64 |
 *   logicalEventId:u64 | payloadLength:u32 | payload | crc32c:u32 | LF:u8
 *
 * frameLength counts from frameType through the trailing delimiter inclusive.
 * CRC32C covers frameType through the final payload byte -- it does NOT cover
 * frameLength, the CRC field itself, or the delimiter.
 */
#define WLR_FRAME_OFF_LENGTH      0
#define WLR_FRAME_OFF_TYPE        4
#define WLR_FRAME_OFF_FLAGS       6
#define WLR_FRAME_OFF_EVENT_SEQ   8
#define WLR_FRAME_OFF_LOGICAL_ID  16
#define WLR_FRAME_OFF_PAYLOAD_LEN 24
#define WLR_FRAME_OFF_PAYLOAD     28

/* Bytes covered by CRC, excluding the payload itself. */
#define WLR_FRAME_CRC_PREFIX_LEN 24
#define WLR_FRAME_CRC_SIZE       4
/* frameLength for an empty payload: body(24) + crc(4) + delimiter(1). */
#define WLR_FRAME_MIN_LENGTH 29

/* TLV: tag:u16 | flags:u16 | length:u32 | value[length]. */
#define WLR_TLV_OFF_TAG     0
#define WLR_TLV_OFF_FLAGS   2
#define WLR_TLV_OFF_LENGTH  4
#define WLR_TLV_OFF_VALUE   8
#define WLR_TLV_HEADER_SIZE WLR_TLV_OFF_VALUE

/*
 * The layout above is the entire cross-binary contract, hand-maintained as offset
 * macros. A single wrong offset would silently corrupt every recording with no
 * diagnostic, so the offsets are checked against each other at compile time: each
 * field must begin exactly where the previous one ends. This cannot catch a layout
 * that is wrong-but-self-consistent, but it does catch the realistic mistake of
 * editing one offset and not its neighbours.
 *
 * Spelled with a negative-size array rather than static_assert: this header is
 * included from both C and C++, and from C compilers older than C11.
 */
#define WLR_LAYOUT_CHECK(_name, _cond)   typedef char wlr_layout_##_name[(_cond) ? 1 : -1]

WLR_LAYOUT_CHECK(magic_len, WLR_HDR_OFF_MAJOR == WLR_HDR_OFF_MAGIC + WLR_FORMAT_MAGIC_LEN);
WLR_LAYOUT_CHECK(major_u16, WLR_HDR_OFF_MINOR == WLR_HDR_OFF_MAJOR + 2);
WLR_LAYOUT_CHECK(minor_u16, WLR_HDR_OFF_ENDIAN == WLR_HDR_OFF_MINOR + 2);
WLR_LAYOUT_CHECK(endian_u16, WLR_HDR_OFF_HDRLEN == WLR_HDR_OFF_ENDIAN + 2);
WLR_LAYOUT_CHECK(hdrlen_u16, WLR_HDR_OFF_SESSION_ID == WLR_HDR_OFF_HDRLEN + 2);
WLR_LAYOUT_CHECK(session_id, WLR_HDR_OFF_START_WALL == WLR_HDR_OFF_SESSION_ID + WLR_SESSION_ID_LEN);
WLR_LAYOUT_CHECK(start_wall_i64, WLR_HDR_OFF_START_MONO == WLR_HDR_OFF_START_WALL + 8);
WLR_LAYOUT_CHECK(start_mono_i64, WLR_HDR_OFF_PID == WLR_HDR_OFF_START_MONO + 8);
WLR_LAYOUT_CHECK(pid_u64, WLR_HDR_OFF_SEGMENT_IDX == WLR_HDR_OFF_PID + 8);
WLR_LAYOUT_CHECK(segment_idx_u32, WLR_HDR_OFF_FIRST_SEQ == WLR_HDR_OFF_SEGMENT_IDX + 4);
WLR_LAYOUT_CHECK(first_seq_u64, WLR_HDR_OFF_CLOCK_DOMAIN == WLR_HDR_OFF_FIRST_SEQ + 8);

WLR_LAYOUT_CHECK(frame_length_u32, WLR_FRAME_OFF_TYPE == WLR_FRAME_OFF_LENGTH + 4);
WLR_LAYOUT_CHECK(frame_type_u16, WLR_FRAME_OFF_FLAGS == WLR_FRAME_OFF_TYPE + 2);
WLR_LAYOUT_CHECK(frame_flags_u16, WLR_FRAME_OFF_EVENT_SEQ == WLR_FRAME_OFF_FLAGS + 2);
WLR_LAYOUT_CHECK(frame_seq_u64, WLR_FRAME_OFF_LOGICAL_ID == WLR_FRAME_OFF_EVENT_SEQ + 8);
WLR_LAYOUT_CHECK(frame_logical_u64, WLR_FRAME_OFF_PAYLOAD_LEN == WLR_FRAME_OFF_LOGICAL_ID + 8);
WLR_LAYOUT_CHECK(frame_paylen_u32, WLR_FRAME_OFF_PAYLOAD == WLR_FRAME_OFF_PAYLOAD_LEN + 4);
/* CRC covers frameType through the last payload byte, so its prefix is everything
 * after frameLength up to the payload. */
WLR_LAYOUT_CHECK(crc_prefix, WLR_FRAME_CRC_PREFIX_LEN == WLR_FRAME_OFF_PAYLOAD - WLR_FRAME_OFF_TYPE);
/* Smallest legal frame: header after frameLength, plus crc32c and the delimiter. */
WLR_LAYOUT_CHECK(frame_min, WLR_FRAME_MIN_LENGTH == WLR_FRAME_CRC_PREFIX_LEN + WLR_FRAME_CRC_SIZE + 1);
WLR_LAYOUT_CHECK(frame_body_tail,
                 WLR_FRAME_MIN_LENGTH - WLR_FRAME_CRC_PREFIX_LEN == WLR_FRAME_CRC_SIZE + 1);
WLR_LAYOUT_CHECK(tlv_tag_u16, WLR_TLV_OFF_FLAGS == WLR_TLV_OFF_TAG + 2);
WLR_LAYOUT_CHECK(tlv_flags_u16, WLR_TLV_OFF_LENGTH == WLR_TLV_OFF_FLAGS + 2);
WLR_LAYOUT_CHECK(tlv_length_u32, WLR_TLV_OFF_VALUE == WLR_TLV_OFF_LENGTH + 4);
WLR_LAYOUT_CHECK(tlv_header, WLR_TLV_HEADER_SIZE == WLR_TLV_OFF_VALUE);

#define WLR_FRAME_DELIMITER 0x0A

/* --------------------------------------------------------------- constants */

/* One frame payload never exceeds this; larger logical events are chunked. */
#define WLR_MAX_FRAME_PAYLOAD (1u << 20) /* 1MiB */

/* Streaming chunk unit, WORKLOAD_CHUNK_SIZE in DS 6.13. CHUNK_DATA frames carry
 * raw bytes, so the entire frame payload is available to the chunk. */
#define WLR_CHUNK_SIZE (1u << 20) /* 1MiB */
#if WLR_CHUNK_SIZE == 0 || WLR_CHUNK_SIZE > WLR_MAX_FRAME_PAYLOAD
#error "WLR_CHUNK_SIZE must fit in one frame payload"
#endif

/* Ordinary segments roll at this size, on logical-event boundaries only. A
 * single logical event larger than this gets a dedicated oversized segment
 * rather than being split across files (DS 6.15.2). */
#define WLR_SEGMENT_ROTATE_SIZE (1ull << 30) /* 1GiB */

/* Writer flush period (DS 6.15.2). Begin-Ack means write() returned, not
 * fsync() -- see DS 6.14. */
#define WLR_WRITER_FLUSH_MS 1000

/* A single logical payload above this cannot be recorded: the session goes
 * fail-open with WLR_E_PAYLOAD_LIMIT (DS 6.13). */
#define WLR_MAX_LOGICAL_PAYLOAD (64ull << 30) /* 64GiB */

/* File naming (DS 6.5). Collision counter increments on exclusive-create
 * failure; overwriting an existing file is forbidden. */
#define WLR_SEGMENT_SUFFIX    ".wlr"
#define WLR_MANIFEST_SUFFIX   ".rr-manifest.json"
#define WLR_SEGMENT_NAME_FMT  "wlr-%" PRId64 "-%" PRIu64 "-%u-%u" WLR_SEGMENT_SUFFIX
#define WLR_MANIFEST_NAME_FMT "wlr-%" PRId64 "-%" PRIu64 "-%u" WLR_MANIFEST_SUFFIX

/* Default config (FS 4.3). */
#define WLR_CFG_MODE_OFF          0
#define WLR_CFG_MODE_ON           1
#define WLR_CFG_MODE_MAX          255
#define WLR_CFG_BUFFER_MB_DEFAULT 256
#define WLR_CFG_BUFFER_MB_MIN     16
#define WLR_CFG_BUFFER_MB_MAX     4096

/* Least-privilege permissions (FS 6). */
#define WLR_DIR_PERM  0700
#define WLR_FILE_PERM 0600

/* ------------------------------------------------------------- frame types */

typedef enum {
  WLR_FRAME_EVENT = 1,        /* a complete single-frame event */
  WLR_FRAME_CHUNK_BEGIN = 2,  /* logical event too large for one frame */
  WLR_FRAME_CHUNK_DATA = 3,
  WLR_FRAME_CHUNK_END = 4,    /* carries the whole-payload CRC */
  WLR_FRAME_SESSION_STOP = 5, /* graceful stop marker + active call list */
  WLR_FRAME_FATAL = 6,        /* fail-open reason, best effort */
} EWlrFrameType;

/* ------------------------------------------------------------- event phase */

typedef enum {
  WLR_PHASE_BEGIN = 1,  /* written and ack'd BEFORE the real API is entered */
  WLR_PHASE_RETURN = 2, /* the API returned to the caller */
  WLR_PHASE_DONE = 3,   /* async completion, before the user callback runs */
} EWlrEventPhase;

/* ---------------------------------------------------------------- TLV tags */

/*
 * TLV: tag:u16 | flags:u16 | length:u32 | value[length]
 *
 * The tag high bit marks "required". A parser MUST reject a frame carrying an
 * unknown required tag, and MUST skip an unknown optional tag by its length --
 * that asymmetry is what makes minor versions forward compatible (DS 6.15.2).
 */
#define WLR_TAG_REQUIRED_BIT 0x8000u
#define WLR_TAG_BASE(t)      ((uint16_t)((t) & (uint16_t)~WLR_TAG_REQUIRED_BIT))
#define WLR_TAG_IS_REQUIRED(t) (((t) & WLR_TAG_REQUIRED_BIT) != 0)

/* Whether a tag number is one this build defines. The argument may be either a raw
 * wire tag or an already-normalized base tag; the function applies WLR_TAG_BASE
 * internally. Needed to honour the rule above: rejecting a frame requires
 * distinguishing a tag this build does not know from one it knows but a given API's
 * schema does not require. Must be updated when a tag is added, which the conformance
 * test enforces. */
bool wlrTagIsKnown(uint16_t tag);

typedef enum {
  /* common event fields */
  WLR_TAG_WALL_NS = 1,
  WLR_TAG_MONO_NS = 2,
  WLR_TAG_ORIGIN_TID = 3,
  WLR_TAG_EMITTER_TID = 4,
  WLR_TAG_API_TYPE = 5,
  WLR_TAG_EVENT_PHASE = 6,
  WLR_TAG_CALL_ID = 7,
  WLR_TAG_PARENT_CALL_ID = 8,
  WLR_TAG_CONN_ID = 9,
  WLR_TAG_STMT_ID = 10,
  WLR_TAG_RESULT_ID = 11,
  WLR_TAG_REQID = 12,
  WLR_TAG_QID = 13,

  /* SQL */
  WLR_TAG_SQL = 100, /* plain UTF-8, explicit length, not NUL-terminated */

  /* connect */
  WLR_TAG_ENDPOINT = 101,
  WLR_TAG_USER = 102,
  WLR_TAG_DB = 103,
  WLR_TAG_PORT = 104,

  /* connection options: one option per event (taos_options_connection) */
  WLR_TAG_CONN_OPT_NAME = 110,
  WLR_TAG_CONN_OPT_TYPE = 111,
  WLR_TAG_CONN_OPT_VALUE = 112,
  /*
   * A whole option SET on one event, for taos_connect_with: it takes an option bag
   * and applies every entry as part of one call the application made once.
   *
   * A single tag holding a list, rather than repeating the three tags above N
   * times. Repeating them would make an option's name, type and value implicitly
   * paired by position, and nothing in the format expresses that: the required-tag
   * validator only asks whether a tag is present, and decoded TLVs are a flat
   * array. One self-describing payload keeps each option's fields together.
   *
   * Part of format 1.0 and encoded as OPTIONAL, so consumers that do not act on
   * option bags skip it by length (DS 11.3).
   */
  WLR_TAG_CONN_OPT_LIST = 113,

  /* STMT bind, encoded column-wise; never a raw bind struct */
  WLR_TAG_BIND_MODE = 120,
  WLR_TAG_BIND_TYPE = 121,
  WLR_TAG_BIND_COUNT = 122,
  WLR_TAG_BIND_NULL = 123,
  WLR_TAG_BIND_LENGTH = 124,
  WLR_TAG_BIND_BYTES = 125,
  /* Target table name for STMT set_tbname. Part of format 1.0 and OPTIONAL because
   * it refines the bind target rather than the common event envelope. */
  WLR_TAG_STMT_TBNAME = 126,
  /* STMT2 BINDV table-name list, encoded with the schemaless line layout (count +
   * per-entry lengths + bytes) since it is the same shape: N strings whose boundaries
   * must survive. Part of format 1.0 and optional, like tag 126. */
  WLR_TAG_STMT2_TBNAMES = 127,
  /*
   * Target column for a single-column bind: taos_stmt_bind_single_param_batch and
   * taos_stmt2_bind_param(_a) all take one.
   *
   * Without it a recorded single-column bind is indistinguishable from a full-row
   * bind of one column, so replay would write the value into the wrong column
   * while appearing to succeed. Signed, because the row-mode APIs pass -1 to mean
   * "all columns" and that is a different instruction from column 0.
   *
   * Part of format 1.0 and optional, like tags 126 and 127: absent means the caller
   * bound every column.
   */
  WLR_TAG_BIND_COL_IDX = 128,
  WLR_TAG_STMT2_ASYNC = 129,

  /* schemaless */
  WLR_TAG_SML_PROTOCOL = 130,
  WLR_TAG_SML_PRECISION = 131,
  WLR_TAG_SML_TTL = 132,
  WLR_TAG_SML_COUNT = 133,
  WLR_TAG_SML_TBNAME_KEY = 134,
  WLR_TAG_SML_PAYLOAD = 135,

  /* session lifecycle */
  WLR_TAG_ACTIVE_CALL_COUNT = 136,
  WLR_TAG_STMT2_TAGS = 137,
  WLR_TAG_STMT_INIT_OPTIONS = 138,
  WLR_TAG_STMT2_NULLABLE_TBNAMES = 139,

  /* results */
  WLR_TAG_RET_CODE = 140,
  WLR_TAG_AFFECTED_ROWS = 141,
  WLR_TAG_FETCH_ROWS = 142,
  WLR_TAG_DURATION_NS = 143,

  /* chunking */
  WLR_TAG_CHUNK_INDEX = 150,
  WLR_TAG_CHUNK_COUNT = 151,
  WLR_TAG_TOTAL_LENGTH = 152,
  WLR_TAG_PAYLOAD_CRC = 153,
} EWlrTag;

#define WLR_STMT_OPT_SINGLE_STB_INSERT 1u
#define WLR_STMT_OPT_SINGLE_TABLE_BIND_ONCE 2u
#define WLR_STMT_OPT_ASYNC_CALLBACK 4u
#define WLR_STMT_OPT_USERDATA 8u

/* ---------------------------------------------------------------- API type */

/*
 * Stable codes for the 57 recorded public APIs plus the 3 auth variants that
 * emit diagnostic-only events (DS 6.7.1). Values are grouped by decade so a
 * later addition never renumbers an existing code -- an on-disk code must mean
 * the same thing forever. Adding one requires a format MINOR bump (DS 11.3).
 *
 * Excluded from this enum on purpose: TMQ, raw block write, all local getters,
 * process-global init/config, route/whitelist, and instance management. Those
 * produce no events at all.
 */
typedef enum {
  WLR_API_UNKNOWN = 0,

  /* connection / session */
  WLR_API_CONNECT = 1,
  WLR_API_CONNECT_WITH = 2,
  WLR_API_CLOSE = 3,
  WLR_API_OPTIONS_CONNECTION = 4,
  WLR_API_SELECT_DB = 5,
  WLR_API_RESET_CURRENT_DB = 6,
  WLR_API_TXN_BEGIN = 7,
  WLR_API_TXN_COMMIT = 8,
  WLR_API_TXN_ROLLBACK = 9,
  WLR_API_KILL_QUERY = 10,
  WLR_API_STOP_QUERY = 11,
  /* diagnostic only: secrets are never stored, so replay substitutes
   * taos_connect and reports AUTH_METHOD_SUBSTITUTED (DS 6.7.1) */
  WLR_API_CONNECT_AUTH = 12,
  WLR_API_CONNECT_TOTP = 13,
  WLR_API_CONNECT_TOKEN = 14,

  /* query */
  WLR_API_QUERY = 20,
  WLR_API_QUERY_WITH_REQID = 21,
  WLR_API_QUERY_A = 22,
  WLR_API_QUERY_A_WITH_REQID = 23,
  WLR_API_VALIDATE_SQL = 24,

  /* fetch / free */
  WLR_API_FETCH_ROW = 30,
  WLR_API_FETCH_BLOCK = 31,
  WLR_API_FETCH_BLOCK_S = 32,
  WLR_API_FETCH_RAW_BLOCK = 33,
  WLR_API_FETCH_ROWS_A = 34,
  WLR_API_FETCH_RAW_BLOCK_A = 35,
  WLR_API_FREE_RESULT = 36,

  /* STMT1 */
  WLR_API_STMT_INIT = 40,
  WLR_API_STMT_INIT_WITH_REQID = 41,
  WLR_API_STMT_INIT_WITH_OPTIONS = 42,
  WLR_API_STMT_PREPARE = 43,
  WLR_API_STMT_SET_TBNAME_TAGS = 44,
  WLR_API_STMT_SET_TBNAME = 45,
  WLR_API_STMT_SET_TAGS = 46,
  WLR_API_STMT_SET_SUB_TBNAME = 47,
  WLR_API_STMT_BIND_PARAM = 48,
  WLR_API_STMT_BIND_PARAM_BATCH = 49,
  WLR_API_STMT_BIND_SINGLE_PARAM_BATCH = 50,
  WLR_API_STMT_ADD_BATCH = 51,
  WLR_API_STMT_EXECUTE = 52,
  WLR_API_STMT_USE_RESULT = 53,
  WLR_API_STMT_CLOSE = 54,

  /* STMT2 */
  WLR_API_STMT2_INIT = 60,
  WLR_API_STMT2_PREPARE = 61,
  WLR_API_STMT2_BIND_PARAM = 62,
  WLR_API_STMT2_BIND_PARAM_A = 63,
  WLR_API_STMT2_BIND_PARAM_COLUMN = 64,
  WLR_API_STMT2_BIND_PARAM_COLUMN_A = 65,
  WLR_API_STMT2_EXEC = 66,
  WLR_API_STMT2_RESULT = 67,
  WLR_API_STMT2_CLOSE = 68,

  /* schemaless */
  WLR_API_SML_INSERT = 80,
  WLR_API_SML_INSERT_WITH_REQID = 81,
  WLR_API_SML_INSERT_RAW = 82,
  WLR_API_SML_INSERT_RAW_WITH_REQID = 83,
  WLR_API_SML_INSERT_TTL = 84,
  WLR_API_SML_INSERT_TTL_WITH_REQID = 85,
  WLR_API_SML_INSERT_RAW_TTL = 86,
  WLR_API_SML_INSERT_RAW_TTL_WITH_REQID = 87,
  WLR_API_SML_INSERT_RAW_TTL_WITH_REQID_TBNAME_KEY = 88,
  WLR_API_SML_INSERT_TTL_WITH_REQID_TBNAME_KEY = 89,

  WLR_API_MAX = 90,
} EWlrApiType;

/* Number of public symbols recorded as replayable calls (DS 6.7.1). The three
 * auth variants above are diagnostic-only and are not counted here. */
#define WLR_RECORDED_API_COUNT 57

/* ------------------------------------------------------------ error codes */

/* Recorder-internal, never surfaced through taos.h (FS 4.13). */
typedef enum {
  WLR_E_OK = 0,
  WLR_E_INIT_DIR = 1,       /* dir create / permission / path validation failed */
  WLR_E_ALLOC = 2,          /* queue or payload pool allocation failed */
  WLR_E_WRITE = 3,          /* segment write/flush/fsync permanently failed */
  WLR_E_PAYLOAD_LIMIT = 4,  /* logical payload above WLR_MAX_LOGICAL_PAYLOAD */
  WLR_E_CONFIG = 5,         /* invalid or reserved startup parameter */
} EWlrError;

/* ---------------------------------------------------------- decode results */

typedef enum {
  WLR_DECODE_OK = 0,
  /* Incomplete trailing frame. LEGAL for an actively-written or copied segment:
   * ignore the tail with a warning. Must NOT mark the session incomplete
   * (FS 4.9, DS 6.15.3). */
  WLR_DECODE_TORN_TAIL = 1,
  /* Frame present but unterminated; treated as a torn tail, not corruption. */
  WLR_DECODE_MISSING_LF = 2,
  /* Mid-file damage: structurallyComplete=false. */
  WLR_DECODE_CRC_MISMATCH = 3,
  WLR_DECODE_BAD_LENGTH = 4,
  WLR_DECODE_BAD_MAGIC = 5,
  WLR_DECODE_BAD_VERSION = 6,
  /* A required tag is unknown, or a tag the API's schema requires is absent:
   * refuse rather than silently drop semantics. Returned by
   * wlrValidateRequiredTags, never by the structural decoders. */
  WLR_DECODE_UNKNOWN_REQUIRED_TAG = 7,
  WLR_DECODE_BAD_ENDIAN = 8,
  /* Structurally valid input with more entries than the caller's output array.
   * tlvCount receives the total required capacity. */
  WLR_DECODE_NO_SPACE = 9,
  /* Caller contract violation such as a required output pointer being NULL. */
  WLR_DECODE_BAD_ARG = 10,
  /* A temporary allocation required for validation failed. */
  WLR_DECODE_ALLOC_FAILED = 11,
} EWlrDecodeResult;

/* ------------------------------------------------------- in-memory structs */

/* IN-MEMORY ONLY. Never memcpy'd to disk: use wlrEncodeFileHeader. */
typedef struct {
  /*
   * DECODE-ONLY outputs. On a non-OK decode return, these fields are undefined and
   * must not be read. wlrEncodeFileHeader ignores whatever a caller puts here and
   * always writes the compile-time WLR_FORMAT_MAJOR / WLR_FORMAT_MINOR and
   * WLR_FILE_HEADER_SIZE.
   *
   * That is deliberate, not an oversight: a writer emits the one version it actually
   * implements, so letting a caller stamp a different number would produce a file whose
   * declared version does not describe its contents. The fields exist because a READER
   * needs them -- wlrDecodeFileHeader fills them in, and headerLength is what lets a
   * reader skip a longer header written by a future minor version. Setting them before
   * an encode has no effect.
   */
  uint16_t major;
  uint16_t minor;
  uint16_t headerLength;
  /* Round-tripped normally: encode writes these, decode reads them back. */
  uint8_t  sessionId[WLR_SESSION_ID_LEN];
  int64_t  processStartWallNs;
  int64_t  processStartMonoNs;
  uint64_t pid;
  uint32_t segmentIndex;
  uint64_t firstEventSeq;
  char     clockDomainId[WLR_CLOCK_DOMAIN_ID_LEN];
} SWlrFileHeader;

/* IN-MEMORY ONLY. Never memcpy'd to disk: use wlrEncodeFrame. */
typedef struct {
  uint16_t type;  /* EWlrFrameType */
  uint16_t flags;
  uint64_t eventSequence;
  uint64_t logicalEventId;
  uint32_t payloadLength;
} SWlrFrameHeader;

typedef struct {
  uint16_t       tag; /* may carry WLR_TAG_REQUIRED_BIT */
  uint16_t       flags;
  uint32_t       length;
  const uint8_t *value; /* borrowed; valid only while the frame buffer lives */
} SWlrTlv;

/* Session completeness, mirrored into the manifest (DS 6.15.1).
 *
 * These four are independent on purpose. In particular hasUnfinishedCalls is
 * NOT a defect: a BEGIN without END is exactly how a hung call is captured, and
 * replay accepts it without --allow-incomplete (DS 6.17). */
typedef struct {
  bool structurallyComplete; /* frames/chunks contiguous, CRCs valid */
  bool captureComplete;      /* no fail-open, no limit hit, no lost events */
  bool gracefullyClosed;     /* SESSION_STOP present and final flush done */
  bool hasUnfinishedCalls;   /* BEGIN with no END at stop time */
} SWlrCompleteness;

/* ------------------------------------------------------- required-tag table */

/*
 * The V1 required-tag table. This is the single source of truth for which tags
 * each (frameType, apiType) pair must carry; the recorder validates before
 * writing and the parser validates after reading, against THIS table. Keeping a
 * second hand-written copy is forbidden (DS 6.15.2).
 */
typedef struct {
  uint16_t        frameType; /* EWlrFrameType */
  uint16_t        apiType;   /* EWlrApiType, or WLR_API_UNKNOWN for any */
  uint16_t        phase;     /* EWlrEventPhase, or 0 for any */
  const uint16_t *tags;      /* base tag numbers, without the required bit */
  int32_t         tagCount;
} SWlrRequiredTags;

/* Lookup returns NULL when no rule constrains the combination. */
const SWlrRequiredTags *wlrGetRequiredTags(uint16_t frameType, uint16_t apiType, uint16_t phase);

/* Self-check on the required-tag table: returns the index of the first wildcard rule
 * that shadows a later specific row or a generated EVENT fallback. Returns -1 when the
 * ordering is sound. wlrGetRequiredTags returns the first match, so a misplaced wildcard
 * silently weakens the selected schema. Exercised by wlrFormatTest rather than at runtime. */
int32_t wlrFindShadowedRule(void);

/* Verify a decoded TLV set satisfies the table. */
EWlrDecodeResult wlrValidateRequiredTags(uint16_t frameType, uint16_t apiType, uint16_t phase,
                                         const SWlrTlv *tlvs, int32_t tlvCount);

/* ------------------------------------------------------- little-endian I/O */

/* Explicit codecs. Do not substitute memcpy of a native integer: that would
 * make the file host-endian (DS 6.15.2). Every buf argument must be non-NULL
 * and point to at least the encoded integer width; these leaf helpers do not
 * perform argument or bounds checks. */
void     wlrPutU16(uint8_t *buf, uint16_t v);
void     wlrPutU32(uint8_t *buf, uint32_t v);
void     wlrPutU64(uint8_t *buf, uint64_t v);
uint16_t wlrGetU16(const uint8_t *buf);
uint32_t wlrGetU32(const uint8_t *buf);
uint64_t wlrGetU64(const uint8_t *buf);

/* ------------------------------------------------------------ encode/decode */

/* Encoding functions that write bytes return the number of bytes written and a
 * negative value on error. The uint32_t size queries wlrBindEncodedSize,
 * wlrSmlLinesEncodedSize, and wlrConnOptListEncodedSize instead use UINT32_MAX for
 * invalid input or an unrepresentable size; callers must reject that sentinel and
 * must never pass it directly to an allocator. */

/* Writes WLR_FILE_HEADER_SIZE bytes. Returns bytes written, or negative on
 * error. */
int32_t wlrEncodeFileHeader(uint8_t *buf, size_t bufLen, const SWlrFileHeader *hdr);
EWlrDecodeResult wlrDecodeFileHeader(const uint8_t *buf, size_t bufLen, SWlrFileHeader *hdr);

/* TLV emission into a caller-owned payload buffer. */
/* Returns the bytes written, or negative on error. A TLV must fit in one frame
 * payload together with its header and the event's other TLVs; this function
 * enforces only the absolute WLR_MAX_FRAME_PAYLOAD value bound and the supplied
 * buffer size. */
int32_t wlrEncodeTlv(uint8_t *buf, size_t bufLen, uint16_t tag, bool required, uint16_t flags,
                     const void *value, uint32_t valueLen);
int32_t wlrEncodeTlvU64(uint8_t *buf, size_t bufLen, uint16_t tag, bool required, uint64_t v);

/*
 * Walk the TLVs of a decoded payload. This is a purely structural parse: every
 * TLV is returned as-is, tags are not interpreted, and the required bit is not
 * examined. Returns WLR_DECODE_BAD_LENGTH on a malformed or truncated payload.
 * Pass tlvs==NULL and tlvCapacity==0 to count first. A negative capacity, or a
 * nonzero capacity with a NULL output array, returns WLR_DECODE_BAD_ARG.
 * When tlvs is non-NULL and tlvCapacity is smaller than the number of entries,
 * returns WLR_DECODE_NO_SPACE without modifying the supplied array and stores the
 * total required capacity in tlvCount. Callers must not use tlvCount as the number
 * of initialized array entries unless the return is WLR_DECODE_OK.
 *
 * Tag semantics are a separate step: call wlrValidateRequiredTags on the result
 * to enforce the per-API required set. A frame is only trustworthy once both
 * have passed -- decoding alone never rejects a frame over its tags.
 */
EWlrDecodeResult wlrDecodeTlvs(const uint8_t *payload, uint32_t payloadLen, SWlrTlv *tlvs,
                               int32_t tlvCapacity, int32_t *tlvCount);

/*
 * Frame emission. Computes the CRC over frameType..payload end and appends the
 * CRC and the trailing LF. The whole frame -- body, CRC and delimiter -- is one
 * write unit; Begin-Ack may only be returned once the LF has been written
 * (DS 6.15.2).
 *
 * payload must reference at least hdr->payloadLength readable bytes. The encoder
 * cannot verify that precondition because the payload buffer length is not passed
 * separately. Returns total bytes written, or negative on error.
 */
int32_t wlrEncodeFrame(uint8_t *buf, size_t bufLen, const SWlrFrameHeader *hdr,
                       const uint8_t *payload);

/* Required buffer size for a payload of the given length. */
size_t wlrFrameEncodedSize(uint32_t payloadLength);

/*
 * Decode one frame. On WLR_DECODE_OK, *consumed is the frame's total size and
 * *payload points into buf, or is NULL when payloadLength is zero. On
 * WLR_DECODE_TORN_TAIL or WLR_DECODE_MISSING_LF the caller has a valid prefix
 * and should stop without flagging corruption. These tail results, like all other
 * non-OK results, do not write output parameters; outputs must only be read on OK.
 */
EWlrDecodeResult wlrDecodeFrame(const uint8_t *buf, size_t bufLen, SWlrFrameHeader *hdr,
                                const uint8_t **payload, uint32_t *consumed);

/* ------------------------------------------------------------- STMT binds */

#define WLR_BIND_FLAG_TRISTATE_NULL 0x0001u
#define WLR_BIND_FLAG_ABSENT_TABLE 0x0002u

/* One input column for encoding. All pointers borrow caller-owned host memory for
 * the duration of wlrBindEncodedSize/wlrEncodeBind. */
typedef struct {
  uint16_t       type;      /* TSDB data type code */
  uint16_t       flags;
  int32_t        elemCount;
  const uint8_t *isNull;    /* elemCount 0/1 bytes, or NULL meaning none are null */
  const int32_t *lengths;   /* host integers; NULL element lengths encode and decode as zero */
  const uint8_t *values;    /* concatenated element bytes */
  uint32_t       valueBytes;
} SWlrBindEncodeCol;

/* One decoded column. Pointers alias the decoded payload and do not outlive it.
 * Fixed-width columns reserve one width-sized value slot per element even when the
 * corresponding isNull byte is set; variable-width NULL elements consume no bytes. */
typedef struct {
  uint16_t       type;
  uint16_t       flags;
  int32_t        elemCount;
  const uint8_t *isNull;
  const uint8_t *lengths; /* little-endian u32 wire values; use wlrBindColLength */
  const uint8_t *values;
  uint32_t       valueBytes;
} SWlrBindCol;

/* True when the type carries an explicit per-element length. */
bool wlrTypeIsVarLen(uint16_t type);

/* Element length at index i of a decoded var-length column, decoded from the
 * file's little-endian representation. Returns 0 when the column carries no
 * length table. The only correct way to read SWlrBindCol::lengths. */
uint32_t wlrBindColLength(const SWlrBindCol *col, int32_t i);

/* True when the type code is one this format defines. wlrTypeIsVarLen cannot
 * answer this on its own: it reports true both for a real variable-length type
 * and for a code it does not recognise, so a decoder needs this to tell a
 * corrupted type from a legitimate one. */
bool wlrTypeIsKnown(uint16_t type);

/* Element width for a fixed-width type, 0 for variable-length ones. */
uint32_t wlrTypeFixedWidth(uint16_t type);

/* Buffer size wlrEncodeBind needs for this column set, or UINT32_MAX when the
 * type/count is invalid or the encoded size is not representable. A finite
 * estimate does not validate value extents or the length table: wlrEncodeBind
 * can still reject inconsistent column contents. Callers must treat UINT32_MAX
 * as an error rather than an allocation size. */
uint32_t wlrBindEncodedSize(const SWlrBindEncodeCol *cols, int32_t colCount);

/* Returns bytes written, or negative when the buffer is too small. */
/* On failure, buf may be partially written and must not be used. */
int32_t wlrEncodeBind(uint8_t *buf, size_t bufLen, const SWlrBindEncodeCol *cols, int32_t colCount);

/*
 * Decode a bind blob. Pass cols==NULL with colCapacity==0 to count columns first.
 * A negative capacity, or a nonzero capacity with cols==NULL, returns
 * WLR_DECODE_BAD_ARG. When cols is non-NULL and colCapacity is too small, returns
 * WLR_DECODE_NO_SPACE without writing past the supplied array. Unlike wlrDecodeTlvs,
 * colCount is only populated on WLR_DECODE_OK; callers that need the required capacity
 * must first call with cols==NULL and colCapacity==0. Returned pointers alias buf and
 * do not outlive it.
 */
/* bufLen must be the exact bind payload length; trailing bytes are rejected. */
EWlrDecodeResult wlrDecodeBind(const uint8_t *buf, uint32_t bufLen, SWlrBindCol *cols,
                               int32_t colCapacity, int32_t *colCount);

/* ------------------------------------------------------- schemaless lines */

/* One schemaless line. Points into the decoded payload and does not outlive it. */
typedef struct {
  const uint8_t *bytes;
  uint32_t       length;
} SWlrSmlLine;

/* Input strings are borrowed and must remain unchanged until the call returns;
 * concurrent mutation is unsupported. Returns UINT32_MAX for invalid input or
 * an unrepresentable encoded size. */
uint32_t wlrSmlLinesEncodedSize(const char *const *lines, int32_t count);
/* The same borrowed-input lifetime applies while encoding. On failure, buf may
 * be partially written and must not be used. */
int32_t  wlrEncodeSmlLines(uint8_t *buf, size_t bufLen, const char *const *lines, int32_t count);
/* Pass out==NULL with capacity==0 to count first. A negative capacity, or a nonzero
 * capacity with out==NULL, returns WLR_DECODE_BAD_ARG. A non-NULL array with
 * insufficient capacity returns WLR_DECODE_NO_SPACE without writing past it. count is
 * only populated on WLR_DECODE_OK, so use the NULL-output count pass to size the array. */
EWlrDecodeResult wlrDecodeSmlLines(const uint8_t *buf, uint32_t bufLen, SWlrSmlLine *out,
                                   int32_t capacity, int32_t *count);

uint32_t wlrStmt2NamesEncodedSize(const char *const *names, int32_t count);
int32_t wlrEncodeStmt2Names(uint8_t *buf, size_t bufLen, const char *const *names, int32_t count);
EWlrDecodeResult wlrDecodeStmt2Names(const uint8_t *buf, uint32_t bufLen, SWlrSmlLine *out,
                                    int32_t capacity, int32_t *count);

/* ---------------------------------------------------- connection option list */

/*
 * Structural bounds on a recorded connection-option type code.
 *
 * Intentionally far wider than today's TSDB_OPTION_CONNECTION range (-1 through 4):
 * a segment written by a newer client may carry an option code this build does not
 * know, and the format promises such a file still decodes (DS 11.3). These bounds
 * exist only to reject a value that cannot be an option code at all -- the signature
 * of a corrupted entry table -- not to police which options exist. Whoever reapplies
 * an option is responsible for deciding whether it can.
 */
/* Encode and decode use the same deliberately wide wire range. It admits
 * future option values without requiring this build to know their meaning;
 * values outside it are rejected as malformed on both paths. */
#define WLR_CONN_OPT_TYPE_MIN (-4096)
#define WLR_CONN_OPT_TYPE_MAX 4096

/*
 * One connection option inside a WLR_TAG_CONN_OPT_LIST payload.
 *
 * hasValue separates "value cleared" (a legal call, taos_options_connection with a
 * NULL arg) from "value is the empty string". On decode, name/value point into the
 * decoded buffer and do not outlive it.
 */
typedef struct {
  int32_t        type;
  const uint8_t *name;
  uint32_t       nameLen;
  const uint8_t *value;
  uint32_t       valueLen;
  bool           hasValue;
} SWlrConnOpt;

/* Names and values are borrowed for the call and must remain unchanged until it
 * returns; concurrent mutation is unsupported. A NULL value means cleared. */
typedef struct {
  int32_t     type;
  const char *name;
  const char *value;
} SWlrConnOptIn;

/* Returns UINT32_MAX for invalid input or an unrepresentable encoded size. */
uint32_t wlrConnOptListEncodedSize(const SWlrConnOptIn *opts, int32_t count);
/* The borrowed strings must remain unchanged during encoding. On failure, buf
 * may be partially written and must not be used. */
int32_t  wlrEncodeConnOptList(uint8_t *buf, size_t bufLen, const SWlrConnOptIn *opts, int32_t count);
/* Pass out==NULL with capacity==0 to count first. A negative capacity, or a nonzero
 * capacity with out==NULL, returns WLR_DECODE_BAD_ARG. A non-NULL array with
 * insufficient capacity returns WLR_DECODE_NO_SPACE without writing past it. count is
 * only populated on WLR_DECODE_OK, so use the NULL-output count pass to size the array. */
EWlrDecodeResult wlrDecodeConnOptList(const uint8_t *buf, uint32_t bufLen, SWlrConnOpt *out,
                                      int32_t capacity, int32_t *count);

/* ---------------------------------------------------------------- chunking */

/* Chunk count for a logical payload, matching what CHUNK_BEGIN must declare.
 * Returns 0 for an empty payload and UINT32_MAX above WLR_MAX_LOGICAL_PAYLOAD. */
uint32_t wlrChunkCount(uint64_t totalLength);

/* True when a valid logical payload must use the streaming chunk path. Invalid
 * lengths above WLR_MAX_LOGICAL_PAYLOAD return false. */
bool wlrNeedsChunking(uint64_t totalLength);

/* ---------------------------------------------------------------- versions */

/* Whether a file's version can be read: major must match exactly, any minor is
 * accepted because minors only add optional tags or new frame types. */
bool wlrVersionSupported(uint16_t major, uint16_t minor);

#ifdef __cplusplus
}
#endif

#endif /* TDENGINE_WLR_FORMAT_H_ */
