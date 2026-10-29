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
 * STMT bind parameter encoding (DS 6.9).
 *
 * DS 6.9 forbids serializing TAOS_MULTI_BIND or TAOS_STMT2_BIND memory directly,
 * and the reason is not stylistic: those structs hold `void *buffer`, `int32_t
 * *length` and `char *is_null` pointers plus compiler-chosen padding. Writing the
 * struct bytes would record host addresses that mean nothing to a replay process,
 * and the padding would differ across compilers and architectures.
 *
 * So a bind is described column-wise instead: for each column, its type, element
 * count, null flags, per-element lengths, and the value bytes those lengths
 * delimit. That is enough for replay to rebuild an equivalent bind array with its
 * own pointers.
 *
 * Wire layout, all integers little-endian:
 *
 *   columnCount:u32
 *   per column:
 *     type:u16 | flags:u16 | elemCount:u32 | nullBytes:u32 | lenBytes:u32 | valBytes:u32
 *     nullFlags[nullBytes]      one byte per element, 0 or 1
 *     lengths[lenBytes]         u32 per element, present only for var-length types
 *     values[valBytes]          concatenated, NULL elements contribute nothing
 *
 * Fixed-length columns omit the length array: the type determines the width, so
 * storing it per element would be redundant and would let the file disagree with
 * itself. Var-length columns need it, since only the caller knows where each
 * element ends.
 */

#include "wlrFormat.h"

/* Public API header only, for the TSDB_DATA_TYPE_* codes a bind blob stores.
 * Not a client internal: taos.h is the same surface taosreplay talks to. */
#include "taos.h"

#define WLR_BIND_OFF_TYPE        0
#define WLR_BIND_OFF_FLAGS       2
#define WLR_BIND_OFF_ELEM_COUNT  4
#define WLR_BIND_OFF_NULL_BYTES  8
#define WLR_BIND_OFF_LEN_BYTES  12
#define WLR_BIND_OFF_VALUE_BYTES 16
#define WLR_BIND_COL_HDR         20

WLR_LAYOUT_CHECK(bind_type_u16, WLR_BIND_OFF_FLAGS == WLR_BIND_OFF_TYPE + 2);
WLR_LAYOUT_CHECK(bind_flags_u16, WLR_BIND_OFF_ELEM_COUNT == WLR_BIND_OFF_FLAGS + 2);
WLR_LAYOUT_CHECK(bind_elem_count_u32, WLR_BIND_OFF_NULL_BYTES == WLR_BIND_OFF_ELEM_COUNT + 4);
WLR_LAYOUT_CHECK(bind_null_bytes_u32, WLR_BIND_OFF_LEN_BYTES == WLR_BIND_OFF_NULL_BYTES + 4);
WLR_LAYOUT_CHECK(bind_len_bytes_u32, WLR_BIND_OFF_VALUE_BYTES == WLR_BIND_OFF_LEN_BYTES + 4);
WLR_LAYOUT_CHECK(bind_value_bytes_u32, WLR_BIND_COL_HDR == WLR_BIND_OFF_VALUE_BYTES + 4);

/*
 * Fixed width for a type, or 0 when the type is variable-length.
 *
 * The codes come from public taos.h rather than common/ttypes.h: this library must
 * not depend on client or common internals (DS 4.2), but taos.h is the published
 * API surface and the same header replay uses, so it is the authority on what a
 * stored type code means. Spelling the codes as literals here is what allowed 20
 * to be treated as a 16-byte DECIMAL128 when taos.h defines it as GEOMETRY.
 *
 * Widths stay explicit: they are the on-disk element sizes and must not silently
 * follow a host struct if one is ever reorganised.
 */
static uint32_t wlrFixedWidth(uint16_t type) {
  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_UTINYINT:
      return 1;
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_USMALLINT:
      return 2;
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_FLOAT:
      return 4;
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_UBIGINT:
    case TSDB_DATA_TYPE_DOUBLE:
    case TSDB_DATA_TYPE_TIMESTAMP:
      return 8;
    default:
      /* VARCHAR, NCHAR, JSON, VARBINARY, DECIMAL/DECIMAL64, BLOB,
       * MEDIUMBLOB, GEOMETRY. */
      return 0;
  }
}

bool wlrTypeIsVarLen(uint16_t type) { return wlrFixedWidth(type) == 0; }

/*
 * Whether a type code is one this format defines. Needed because wlrFixedWidth
 * reports 0 both for a genuine variable-length type and for a code it does not
 * recognise, so a decoder cannot use it alone to tell a corrupted type from a
 * legitimate var-length one.
 *
 * The variable-length codes are listed explicitly for the same reason the width
 * table is local: the on-disk meaning of a type code must stay pinned even if an
 * internal table is reorganised.
 */
bool wlrTypeIsKnown(uint16_t type) {
  if (wlrFixedWidth(type) != 0) return true;
  switch (type) {
    /* taos.h currently aliases VARCHAR and BINARY to the same numeric code. If
     * that API ever assigns BINARY a distinct value, add it explicitly here. */
    case TSDB_DATA_TYPE_VARCHAR: /* == TSDB_DATA_TYPE_BINARY */
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_JSON:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_DECIMAL:
    case TSDB_DATA_TYPE_DECIMAL64:
    case TSDB_DATA_TYPE_BLOB:
    case TSDB_DATA_TYPE_MEDIUMBLOB:
    case TSDB_DATA_TYPE_GEOMETRY:
      return true;
    default:
      return false;
  }
}

/*
 * Public width accessor. Needed because TAOS_STMT2_BIND, unlike TAOS_MULTI_BIND,
 * carries no buffer_length: for a fixed-width column the type is the only source of
 * the element size, so a caller cannot compute the value extent without this.
 */
uint32_t wlrTypeFixedWidth(uint16_t type) { return wlrFixedWidth(type); }

static bool wlrUntypedBindIsValid(uint16_t flags, int32_t elemCount,
                                  const uint8_t *nullFlags, uint32_t valueBytes) {
  if (valueBytes != 0) return false;
  if ((flags & WLR_BIND_FLAG_ABSENT_TABLE) != 0) return elemCount == 0;
  return elemCount == 1 && nullFlags != NULL &&
         (nullFlags[0] == 1 ||
          ((flags & WLR_BIND_FLAG_TRISTATE_NULL) != 0 && nullFlags[0] == 2));
}

/*
 * Upper bound on the encoded size, for sizing a buffer before encoding.
 *
 * Accumulated in uint64_t and saturated: a var-length column with a huge
 * elemCount would overflow a uint32_t accumulator (elemCount * 4 alone passes
 * 4 GiB near 1e9 elements) and wrap to a small number, so a caller would
 * allocate a buffer far too small and lose the bind. Returning the saturated
 * value instead means the caller's "does it fit" test fails, which is the
 * same answer wlrEncodeBind gives.
 */
uint32_t wlrBindEncodedSize(const SWlrBindEncodeCol *cols, int32_t colCount) {
  uint64_t total = 4;
  int32_t  i;

  if (colCount < 0) return UINT32_MAX;
  if (colCount == 0) return (uint32_t)total;
  if (cols == NULL) return UINT32_MAX;

  for (i = 0; i < colCount; ++i) {
    const SWlrBindEncodeCol *c = &cols[i];
    if (!wlrTypeIsKnown(c->type) &&
        !(c->type == 0 && wlrUntypedBindIsValid(c->flags, c->elemCount,
                                               c->isNull, c->valueBytes))) return UINT32_MAX;
    uint32_t           width = wlrFixedWidth(c->type);

    /* A negative count is not encodable; wlrEncodeBind rejects it outright, so
     * the estimate must not silently report a plausible size for it. */
    if (c->elemCount < 0) return UINT32_MAX;
    if (c->valueBytes > 0 && c->values == NULL) return UINT32_MAX;
    if (width != 0 && (uint64_t)c->valueBytes != (uint64_t)c->elemCount * width) {
      return UINT32_MAX;
    }

    total += WLR_BIND_COL_HDR;
    total += (uint64_t)c->elemCount;                      /* null flags */
    if (width == 0) total += (uint64_t)c->elemCount * 4;  /* lengths */
    total += c->valueBytes;

    if (total > UINT32_MAX) return UINT32_MAX;
  }
  return (uint32_t)total;
}

int32_t wlrEncodeBind(uint8_t *buf, size_t bufLen, const SWlrBindEncodeCol *cols, int32_t colCount) {
  size_t  off = 0;
  int32_t i;
  int32_t e;

  if (buf == NULL || colCount < 0 || (cols == NULL && colCount > 0)) return -1;
  if (bufLen < 4) return -1;

  wlrPutU32(buf, (uint32_t)colCount);
  off = 4;

  for (i = 0; i < colCount; ++i) {
    const SWlrBindEncodeCol *c = &cols[i];
    uint32_t           width = wlrFixedWidth(c->type);
    uint32_t           nullBytes;
    uint32_t           lenBytes;
    uint64_t           lenBytes64;

    if (c->elemCount < 0) return -1;

    /* Reject a type this build does not define, rather than writing it as a blob.
     * wlrFixedWidth reports 0 both for a genuine var-length type and for an
     * unrecognised code, so treating width == 0 as "variable length" would encode an
     * unknown type cleanly and leave an unconditionally unreplayable payload whose
     * failure surfaces on someone else's machine. wlrDecodeBind classifies such an
     * unknown type as malformed input; output-capacity exhaustion remains the distinct
     * WLR_DECODE_NO_SPACE result used by count-then-fill callers. Failing here instead
     * puts the problem where it can be seen: this encoder returns -1 and the caller
     * can discard or report the rejected bind instead of emitting invalid bytes. */
    if (!wlrTypeIsKnown(c->type) &&
        !(c->type == 0 && wlrUntypedBindIsValid(c->flags, c->elemCount,
                                               c->isNull, c->valueBytes))) return -1;

    /* Computed in uint64_t: elemCount * 4 wraps a uint32_t past ~1.07e9 elements,
     * and an understated lenBytes would let the bounds check below pass while the
     * length-writing loop still ran elemCount iterations -- writing past buf.
     * wlrBindEncodedSize saturates for the same reason, and wlrDecodeBind rejects
     * a table whose size does not match its element count. */
    nullBytes = (uint32_t)c->elemCount;
    lenBytes64 = (width == 0) ? (uint64_t)c->elemCount * 4 : 0;
    if (lenBytes64 > UINT32_MAX) return -1;
    lenBytes = (uint32_t)lenBytes64;

    /* Widened so the sum itself cannot wrap before the comparison. */
    if ((uint64_t)off + WLR_BIND_COL_HDR + nullBytes + lenBytes + c->valueBytes > (uint64_t)bufLen) {
      return -1;
    }
    if (c->valueBytes > 0 && c->values == NULL) return -1;

    /* A variable-length column is only recoverable if the per-element lengths
     * delimit the value bytes. Recording value bytes with lengths that do not
     * describe them loses the data while reporting success, so reject instead:
     * a dropped call is visible, silently empty binds are not (FS 4.8).
     *
     *   - no lengths array: nothing delimits the bytes (unless all NULL).
     *   - negative length: would be recorded as a huge u32 by the cast below.
     *   - sum != valueBytes: the two disagree about the value extent.
     *
     * Run this whenever the column is var-length, not only when valueBytes > 0:
     * a caller supplying elemCount > 0 with valueBytes == 0 but non-zero lengths
     * would skip this check, then write those lengths while emitting no value
     * bytes -- replay would rebuild a bind whose length table contradicts the
     * empty value region. */
    if (width == 0) {
      uint64_t sum = 0;

      if (c->lengths == NULL) {
        if (c->valueBytes > 0) return -1;
        for (e = 0; e < c->elemCount; ++e) {
          if (c->isNull == NULL || c->isNull[e] == 0) return -1;
        }
      }

      if (c->lengths != NULL) {
        for (e = 0; e < c->elemCount; ++e) {
          if (c->isNull != NULL && c->isNull[e] != 0) continue;
          if (c->lengths[e] < 0) return -1;
          sum += (uint64_t)c->lengths[e];
        }
      }
      if (sum != (uint64_t)c->valueBytes) return -1;
    } else if ((uint64_t)c->valueBytes != (uint64_t)c->elemCount * width) {
      /* Fixed-width columns carry no length table, so the type width is the ONLY
       * thing that partitions the value region on replay (wlrTypeFixedWidth). An
       * understated valueBytes would therefore make replay read past the recorded
       * bytes while rebuilding the bind. Fail closed, exactly as the var-length
       * branch above does when the lengths and valueBytes disagree. */
      return -1;
    }

    wlrPutU16(buf + off + WLR_BIND_OFF_TYPE, c->type);
    wlrPutU16(buf + off + WLR_BIND_OFF_FLAGS, c->flags);
    wlrPutU32(buf + off + WLR_BIND_OFF_ELEM_COUNT, (uint32_t)c->elemCount);
    wlrPutU32(buf + off + WLR_BIND_OFF_NULL_BYTES, nullBytes);
    wlrPutU32(buf + off + WLR_BIND_OFF_LEN_BYTES, lenBytes);
    wlrPutU32(buf + off + WLR_BIND_OFF_VALUE_BYTES, c->valueBytes);
    off += WLR_BIND_COL_HDR;

    /* Null flags are always written, one byte per element. A caller that passes
     * no is_null array means "nothing is null", which is recorded explicitly
     * rather than implied by absence -- replay must not have to guess. */
    for (e = 0; e < c->elemCount; ++e) {
      uint8_t nullState = c->isNull != NULL ? c->isNull[e] : 0;
      if ((c->flags & WLR_BIND_FLAG_TRISTATE_NULL) != 0) {
        if (nullState > 2) return -1;
        buf[off + (size_t)e] = nullState;
      } else {
        buf[off + (size_t)e] = nullState != 0 ? 1 : 0;
      }
    }
    off += nullBytes;

    if (lenBytes > 0) {
      for (e = 0; e < c->elemCount; ++e) {
        uint32_t elemLen = 0;
        if (c->isNull == NULL || c->isNull[e] == 0) {
          elemLen = (c->lengths != NULL) ? (uint32_t)c->lengths[e] : 0;
        }
        wlrPutU32(buf + off + (size_t)e * 4, elemLen);
      }
      off += lenBytes;
    }

    if (c->valueBytes > 0) {
      memcpy(buf + off, c->values, c->valueBytes);
      off += c->valueBytes;
    }
  }

  if (off > INT32_MAX) return -1;
  return (int32_t)off;
}

/*
 * The stored lengths are little-endian u32 and the buffer offset is arbitrary, so
 * a host int32_t read is wrong on a big-endian machine and unaligned on any of
 * them. Decoding through wlrGetU32 is the only portable access.
 */
uint32_t wlrBindColLength(const SWlrBindCol *col, int32_t i) {
  if (col == NULL || col->lengths == NULL) return 0;
  if (i < 0 || i >= col->elemCount) return 0;
  return wlrGetU32(col->lengths + (size_t)i * 4);
}

EWlrDecodeResult wlrDecodeBind(const uint8_t *buf, uint32_t bufLen, SWlrBindCol *cols,
                               int32_t colCapacity, int32_t *colCount) {
  uint32_t off = 0;
  uint32_t n = 0;
  uint32_t i;

  if (colCount != NULL) *colCount = 0;
  if (colCapacity < 0 || (cols == NULL && colCapacity != 0)) return WLR_DECODE_BAD_ARG;
  if (buf == NULL) return WLR_DECODE_BAD_ARG;
  if (bufLen < 4) return WLR_DECODE_BAD_LENGTH;

  n = wlrGetU32(buf);
  off = 4;

  /* A hostile column count must not be trusted: every column needs at least a
   * header, so anything above that bound is malformed rather than merely large. */
  if (n > 0 && (uint64_t)n * WLR_BIND_COL_HDR > (uint64_t)(bufLen - off)) {
    return WLR_DECODE_BAD_LENGTH;
  }
  if (cols != NULL && (int32_t)n > colCapacity) return WLR_DECODE_NO_SPACE;

  for (i = 0; i < n; ++i) {
    uint16_t type;
    uint16_t flags;
    uint32_t elemCount;
    uint32_t nullBytes;
    uint32_t lenBytes;
    uint32_t valBytes;

    if (off > bufLen || bufLen - off < WLR_BIND_COL_HDR) return WLR_DECODE_BAD_LENGTH;
    type = wlrGetU16(buf + off + WLR_BIND_OFF_TYPE);
    flags = wlrGetU16(buf + off + WLR_BIND_OFF_FLAGS);
    elemCount = wlrGetU32(buf + off + WLR_BIND_OFF_ELEM_COUNT);
    nullBytes = wlrGetU32(buf + off + WLR_BIND_OFF_NULL_BYTES);
    lenBytes = wlrGetU32(buf + off + WLR_BIND_OFF_LEN_BYTES);
    valBytes = wlrGetU32(buf + off + WLR_BIND_OFF_VALUE_BYTES);
    off += WLR_BIND_COL_HDR;

    /* An unrecognised type must not be guessed at. wlrFixedWidth returns 0 for
     * anything it does not know, which is indistinguishable from a genuine
     * variable-length type, so a corrupted code would silently flip a column
     * between the fixed and var-length layouts and replay would rebuild it with
     * the wrong element widths. */
    if (!wlrTypeIsKnown(type) && type != 0) return WLR_DECODE_BAD_LENGTH;

    /* elemCount is published through SWlrBindCol as int32_t, so a value above
     * INT32_MAX would reach the caller as a negative array count. It also feeds
     * the elemCount * 4 product below, which would wrap. Reject it here so both
     * problems are impossible rather than merely unlikely. */
    if (elemCount > (uint32_t)INT32_MAX) return WLR_DECODE_BAD_LENGTH;

    /* Cross-check the declared sizes against the type before using them, so a
     * mismatched file is rejected instead of silently misread. */
    if (nullBytes != elemCount) return WLR_DECODE_BAD_LENGTH;
    if (wlrFixedWidth(type) == 0) {
      if (lenBytes / 4 != elemCount || lenBytes % 4 != 0) return WLR_DECODE_BAD_LENGTH;
      if (lenBytes == 0 && valBytes != 0) return WLR_DECODE_BAD_LENGTH;
    } else if (lenBytes != 0) {
      return WLR_DECODE_BAD_LENGTH;
    } else if ((uint64_t)valBytes != (uint64_t)elemCount * wlrFixedWidth(type)) {
      return WLR_DECODE_BAD_LENGTH;
    }
    /* Subtraction, never addition: the sum could overflow on a crafted file. */
    if (off > bufLen || bufLen - off < nullBytes) return WLR_DECODE_BAD_LENGTH;
    if (nullBytes > bufLen - off || lenBytes > bufLen - off - nullBytes) {
      return WLR_DECODE_BAD_LENGTH;
    }
    if (valBytes > bufLen - off - nullBytes - lenBytes) return WLR_DECODE_BAD_LENGTH;
    if (type == 0 && !wlrUntypedBindIsValid(flags, (int32_t)elemCount,
                                           buf + off, valBytes)) return WLR_DECODE_BAD_LENGTH;
    if ((flags & WLR_BIND_FLAG_ABSENT_TABLE) != 0 && type != 0) return WLR_DECODE_BAD_LENGTH;

    {
      const uint8_t *nullFlags = buf + off;
      uint32_t       e;
      for (e = 0; e < elemCount; ++e) {
        uint8_t maxNullState = (flags & WLR_BIND_FLAG_TRISTATE_NULL) != 0 ? 2 : 1;
        if (nullFlags[e] > maxNullState) return WLR_DECODE_BAD_LENGTH;
      }
    }

    /* The length table must delimit exactly the value region, mirroring the check
     * wlrEncodeBind applies when writing it. Dimensions alone are not enough: a
     * corrupted or crafted file can carry elemCount well-formed lengths whose sum
     * exceeds valBytes, and replay takes each element's extent from those lengths
     * (wlrBindColLength) -- so it would copy past the recorded bytes. Reject the
     * event instead of misreplaying it.
     *
     * NULL elements contribute nothing, matching the encoder, which skips them
     * when summing. */
    if (lenBytes > 0) {
      const uint8_t *nullFlags = buf + off;
      const uint8_t *lenTable = buf + off + nullBytes;
      uint64_t       sum = 0;
      uint32_t       e;

      for (e = 0; e < elemCount; ++e) {
        int32_t elemLen;
        if (nullFlags[e] != 0) {
          if (wlrGetU32(lenTable + (size_t)e * 4) != 0) return WLR_DECODE_BAD_LENGTH;
          continue;
        }
        elemLen = (int32_t)wlrGetU32(lenTable + (size_t)e * 4);
        if (elemLen < 0) return WLR_DECODE_BAD_LENGTH;
        sum += (uint64_t)elemLen;
      }
      if (sum != (uint64_t)valBytes) return WLR_DECODE_BAD_LENGTH;
    }

    if (cols != NULL) {
      SWlrBindCol *c = &cols[i];
      c->type = type;
      c->flags = flags;
      c->elemCount = (int32_t)elemCount;
      c->isNull = buf + off;
      c->lengths = (lenBytes > 0) ? (buf + off + nullBytes) : NULL;
      c->values = (valBytes > 0) ? (buf + off + nullBytes + lenBytes) : NULL;
      c->valueBytes = valBytes;
    }

    off += nullBytes + lenBytes + valBytes;
  }

  if (off != bufLen) return WLR_DECODE_BAD_LENGTH;
  if (colCount != NULL) *colCount = (int32_t)n;
  return WLR_DECODE_OK;
}

/* ------------------------------------------------------- schemaless lines */

/*
 * Schemaless line arrays (DS 6.10: "payload item count; explicit length and
 * raw bytes").
 *
 * The line-based API takes char *lines[] -- N separately allocated strings with no
 * contiguous buffer to point at. Recording only the count would leave the payload
 * unreplayable, and concatenating without lengths would lose the boundaries, since
 * a line may legitimately contain a newline inside a quoted field.
 *
 * So the same shape as a var-length bind column is used: a count, then explicit
 * per-entry lengths, then the bytes those lengths delimit.
 *
 *   entryCount:u32 | lengths[entryCount]:u32 | concatenated bytes
 */
static uint32_t wlrStringListEncodedSize(const char *const *lines, int32_t count, bool nullable) {
  uint64_t total = 4;
  int32_t  i;

  if (count < 0) return UINT32_MAX;
  if (count == 0) return (uint32_t)total;
  if (lines == NULL && !nullable) return UINT32_MAX;

  /* Accumulated in uint64_t and saturated, like wlrBindEncodedSize: this value is
   * the ONLY bounds guard wlrEncodeSmlLines applies before its memcpy loop, so a
   * uint32_t wrap here would let the guard pass and the copy run past bufLen.
   * count * 4 alone wraps past ~1e9 entries. */
  total += (uint64_t)count * 4;
  if (total > UINT32_MAX) return UINT32_MAX;
  for (i = 0; i < count; ++i) {
    if (lines == NULL || lines[i] == NULL) {
      if (!nullable) return UINT32_MAX;
      continue;
    }
    total += (uint64_t)strlen(lines[i]);
    if (total > UINT32_MAX) return UINT32_MAX;
  }
  if (total > UINT32_MAX) return UINT32_MAX;
  return (uint32_t)total;
}

static int32_t wlrEncodeStringList(uint8_t *buf, size_t bufLen, const char *const *lines,
                                  int32_t count, bool nullable) {
  size_t   off;
  int32_t  i;
  uint32_t lenTableOff;

  if (buf == NULL || count < 0 || (lines == NULL && count > 0 && !nullable)) return -1;
  if (bufLen < 4) return -1;
  lenTableOff = 4;
  if ((size_t)count > (SIZE_MAX - lenTableOff) / 4) return -1;
  off = (size_t)lenTableOff + (size_t)count * 4;
  if (off > bufLen || off > INT32_MAX) return -1;
  wlrPutU32(buf, (uint32_t)count);

  for (i = 0; i < count; ++i) {
    /* strlen is size_t; a line above UINT32_MAX would be truncated in the length
     * table while wlrSmlLinesEncodedSize counted the full extent, so the record would
     * describe fewer bytes than it holds. Reject instead, as everywhere else here. */
    if (lines == NULL || lines[i] == NULL) {
      if (!nullable) return -1;
      wlrPutU32(buf + lenTableOff + (size_t)i * 4, UINT32_MAX);
      continue;
    }
    size_t   lineLen = strlen(lines[i]);
    uint32_t n;
    if (lineLen >= UINT32_MAX) return -1;
    n = (uint32_t)lineLen;
    if (off > bufLen || (size_t)n > bufLen - off) return -1;
    wlrPutU32(buf + lenTableOff + (size_t)i * 4, n);
    off += n;
    if (off > INT32_MAX) return -1;
  }
  off = (size_t)lenTableOff + (size_t)count * 4;
  for (i = 0; i < count; ++i) {
    if (lines == NULL || lines[i] == NULL) {
      if (!nullable || wlrGetU32(buf + lenTableOff + (size_t)i * 4) != UINT32_MAX) return -1;
      continue;
    }
    size_t lineLen = strlen(lines[i]);
    if (lineLen != wlrGetU32(buf + lenTableOff + (size_t)i * 4)) return -1;
    if (off > bufLen || lineLen > bufLen - off) return -1;
    if (lineLen > 0) {
      memcpy(buf + off, lines[i], lineLen);
      off += lineLen;
    }
  }
  return (int32_t)off;
}

static EWlrDecodeResult wlrDecodeStringList(const uint8_t *buf, uint32_t bufLen, SWlrSmlLine *out,
                                           int32_t capacity, int32_t *count, bool nullable) {
  uint32_t n;
  size_t   off;
  uint32_t i;

  if (count != NULL) *count = 0;
  if (capacity < 0 || (out == NULL && capacity != 0)) return WLR_DECODE_BAD_ARG;
  if (buf == NULL) return WLR_DECODE_BAD_ARG;
  if (bufLen < 4) return WLR_DECODE_BAD_LENGTH;

  n = wlrGetU32(buf);
  /* Each entry needs 4 length bytes, so anything above that bound is malformed
   * rather than merely large. */
  if (n > (bufLen - 4) / 4) return WLR_DECODE_BAD_LENGTH;
  if (n > (uint32_t)INT32_MAX) return WLR_DECODE_BAD_LENGTH;
  if (out != NULL && (int32_t)n > capacity) return WLR_DECODE_NO_SPACE;

  off = 4u + (size_t)n * 4u;
  for (i = 0; i < n; ++i) {
    uint32_t len = wlrGetU32(buf + 4 + i * 4);
    if (nullable && len == UINT32_MAX) {
      if (out != NULL) {
        out[i].bytes = NULL;
        out[i].length = 0;
      }
      continue;
    }
    /* Subtraction, never addition: a crafted length could overflow the sum. */
    if (off > bufLen || (size_t)len > (size_t)bufLen - off) return WLR_DECODE_BAD_LENGTH;
    if (out != NULL) {
      out[i].bytes = (len > 0 || nullable) ? (buf + off) : NULL;
      out[i].length = len;
    }
    off += len;
  }

  if (off != bufLen) return WLR_DECODE_BAD_LENGTH;
  if (count != NULL) *count = (int32_t)n;
  return WLR_DECODE_OK;
}

uint32_t wlrSmlLinesEncodedSize(const char *const *lines, int32_t count) {
  return wlrStringListEncodedSize(lines, count, false);
}

int32_t wlrEncodeSmlLines(uint8_t *buf, size_t bufLen, const char *const *lines, int32_t count) {
  return wlrEncodeStringList(buf, bufLen, lines, count, false);
}

EWlrDecodeResult wlrDecodeSmlLines(const uint8_t *buf, uint32_t bufLen, SWlrSmlLine *out,
                                  int32_t capacity, int32_t *count) {
  return wlrDecodeStringList(buf, bufLen, out, capacity, count, false);
}

uint32_t wlrStmt2NamesEncodedSize(const char *const *names, int32_t count) {
  return wlrStringListEncodedSize(names, count, true);
}

int32_t wlrEncodeStmt2Names(uint8_t *buf, size_t bufLen, const char *const *names, int32_t count) {
  return wlrEncodeStringList(buf, bufLen, names, count, true);
}

EWlrDecodeResult wlrDecodeStmt2Names(const uint8_t *buf, uint32_t bufLen, SWlrSmlLine *out,
                                    int32_t capacity, int32_t *count) {
  return wlrDecodeStringList(buf, bufLen, out, capacity, count, true);
}

/*
 * Connection option list (WLR_TAG_CONN_OPT_LIST).
 *
 * taos_connect_with applies a whole option bag as part of one call, so all of them
 * have to travel on one event. Same self-describing shape as the two encoders
 * above -- a count, then a fixed-size entry table, then the bytes those entries
 * delimit:
 *
 *   entryCount:u32 | entries[entryCount] | concatenated name+value bytes
 *
 * where each entry is:
 *
 *   type:i32 | nameLen:u32 | valueLen:u32 | hasValue:u32
 *
 * hasValue is its own field rather than valueLen==0, because clearing an option
 * (NULL value) and setting it to "" are different calls and replay must be able to
 * tell them apart.
 */
#define WLR_CONN_OPT_ENTRY_SIZE 16

uint32_t wlrConnOptListEncodedSize(const SWlrConnOptIn *opts, int32_t count) {
  uint64_t total = 4;
  int32_t  i;

  if (count < 0) return UINT32_MAX;
  if (count == 0) return (uint32_t)total;
  if (opts == NULL) return UINT32_MAX;

  /* Saturating for the same reason as wlrSmlLinesEncodedSize: wlrEncodeConnOptList
   * bounds-checks solely against this value before copying. */
  total += (uint64_t)count * WLR_CONN_OPT_ENTRY_SIZE;
  if (total > UINT32_MAX) return UINT32_MAX;
  for (i = 0; i < count; ++i) {
    if (opts[i].name == NULL || opts[i].name[0] == '\0') return UINT32_MAX;
    total += (uint64_t)strlen(opts[i].name);
    if (opts[i].value != NULL) total += (uint64_t)strlen(opts[i].value);
    if (total > UINT32_MAX) return UINT32_MAX;
  }
  if (total > UINT32_MAX) return UINT32_MAX;
  return (uint32_t)total;
}

int32_t wlrEncodeConnOptList(uint8_t *buf, size_t bufLen, const SWlrConnOptIn *opts, int32_t count) {
  size_t   off;
  int32_t  i;
  uint32_t entryOff;

  if (buf == NULL || count < 0 || (opts == NULL && count > 0)) return -1;
  if (bufLen < 4) return -1;
  entryOff = 4;
  if ((size_t)count > (SIZE_MAX - entryOff) / WLR_CONN_OPT_ENTRY_SIZE) return -1;
  off = (size_t)entryOff + (size_t)count * WLR_CONN_OPT_ENTRY_SIZE;
  if (off > bufLen) return -1;
  wlrPutU32(buf, (uint32_t)count);

  for (i = 0; i < count; ++i) {
    /* Same truncation hazard as wlrEncodeSmlLines: a name or value above UINT32_MAX
     * would be written short while the size estimate used the full length. */
    size_t   nameLenSz = (opts[i].name != NULL) ? strlen(opts[i].name) : 0;
    size_t   valueLenSz = (opts[i].value != NULL) ? strlen(opts[i].value) : 0;
    uint32_t nameLen;
    uint32_t valueLen;
    uint8_t *e;
    if (nameLenSz == 0) return -1;
    if (nameLenSz > UINT32_MAX || valueLenSz > UINT32_MAX) return -1;
    nameLen = (uint32_t)nameLenSz;
    valueLen = (uint32_t)valueLenSz;
    /* Validated here, not only on decode. wlrDecodeConnOptList rejects a type outside
     * this range, so writing one would produce a segment that encodes cleanly and is
     * then unreplayable -- the same encode/decode asymmetry wlrEncodeBind avoids by
     * calling wlrTypeIsKnown. Fail at record time, where it can be seen. */
    if (opts[i].type < WLR_CONN_OPT_TYPE_MIN || opts[i].type > WLR_CONN_OPT_TYPE_MAX) return -1;
    e = buf + entryOff + (size_t)i * WLR_CONN_OPT_ENTRY_SIZE;

    wlrPutU32(e, (uint32_t)opts[i].type);
    wlrPutU32(e + 4, nameLen);
    wlrPutU32(e + 8, valueLen);
    wlrPutU32(e + 12, (opts[i].value != NULL) ? 1u : 0u);

    if (off > bufLen || (size_t)nameLen > bufLen - off) return -1;
    off += nameLen;
    if (off > bufLen || (size_t)valueLen > bufLen - off) return -1;
    off += valueLen;
    if (off > INT32_MAX) return -1;
  }

  off = (size_t)entryOff + (size_t)count * WLR_CONN_OPT_ENTRY_SIZE;
  for (i = 0; i < count; ++i) {
    const uint8_t *e = buf + entryOff + (size_t)i * WLR_CONN_OPT_ENTRY_SIZE;
    if (opts[i].name == NULL) return -1;
    size_t nameLen = strlen(opts[i].name);
    size_t valueLen = (opts[i].value != NULL) ? strlen(opts[i].value) : 0;
    if (nameLen != wlrGetU32(e + 4) || valueLen != wlrGetU32(e + 8) ||
        (opts[i].value != NULL ? 1u : 0u) != wlrGetU32(e + 12)) {
      return -1;
    }

    if (off > bufLen || nameLen > bufLen - off) return -1;
    if (nameLen > 0) {
      memcpy(buf + off, opts[i].name, nameLen);
      off += nameLen;
    }
    if (off > bufLen || valueLen > bufLen - off) return -1;
    if (valueLen > 0) {
      memcpy(buf + off, opts[i].value, valueLen);
      off += valueLen;
    }
  }
  return (int32_t)off;
}

EWlrDecodeResult wlrDecodeConnOptList(const uint8_t *buf, uint32_t bufLen, SWlrConnOpt *out,
                                      int32_t capacity, int32_t *count) {
  uint32_t n;
  uint32_t off;
  uint32_t i;

  if (count != NULL) *count = 0;
  if (capacity < 0 || (out == NULL && capacity != 0)) return WLR_DECODE_BAD_ARG;
  if (buf == NULL) return WLR_DECODE_BAD_ARG;
  if (bufLen < 4) return WLR_DECODE_BAD_LENGTH;

  n = wlrGetU32(buf);
  /* Each entry needs its fixed table slot, so anything above that bound is
   * malformed rather than merely large. */
  if (n > (bufLen - 4) / WLR_CONN_OPT_ENTRY_SIZE) return WLR_DECODE_BAD_LENGTH;
  if (n > (uint32_t)INT32_MAX) return WLR_DECODE_BAD_LENGTH;
  if (out != NULL && (int32_t)n > capacity) return WLR_DECODE_NO_SPACE;

  off = 4u + (size_t)n * WLR_CONN_OPT_ENTRY_SIZE;
  for (i = 0; i < n; ++i) {
    const uint8_t *e = buf + 4 + (size_t)i * WLR_CONN_OPT_ENTRY_SIZE;
    int32_t        optType = (int32_t)wlrGetU32(e);
    uint32_t       nameLen = wlrGetU32(e + 4);
    uint32_t       valueLen = wlrGetU32(e + 8);
    uint32_t       hasValueRaw = wlrGetU32(e + 12);
    bool           hasValue;

    if (hasValueRaw > 1) return WLR_DECODE_BAD_LENGTH;
    hasValue = (hasValueRaw != 0);

    /* Structural sanity on the type code. Deliberately a wide range rather than an
     * exact allow-list of today's TSDB_OPTION_CONNECTION members: a segment written
     * by a NEWER client may legitimately carry an option this build has no name for,
     * and rejecting the whole list for that would break the forward compatibility the
     * format promises (DS 11.3). What this does catch is a wildly out-of-range value,
     * the signature of a corrupted or crafted entry table. Semantic validation of the
     * code belongs to whoever reapplies the option, not to the decoder -- taosReplay
     * does not currently reissue these calls at all. */
    if (optType < WLR_CONN_OPT_TYPE_MIN || optType > WLR_CONN_OPT_TYPE_MAX) {
      return WLR_DECODE_BAD_LENGTH;
    }

    /* hasValue exists solely to separate "cleared" from "set to empty", so the two
     * fields must agree: a cleared option carries no bytes. Without this a crafted
     * file could present hasValue=false alongside valueLen>0, and replay would clear
     * an option the file also describes a value for -- changing connection semantics
     * from a corrupted input rather than rejecting it. */
    if (!hasValue && valueLen != 0) return WLR_DECODE_BAD_LENGTH;

    /* Subtraction, never addition: a crafted length could overflow the sum. */
    if (off > bufLen || (size_t)bufLen - off < nameLen) return WLR_DECODE_BAD_LENGTH;
    if (nameLen == 0) return WLR_DECODE_BAD_LENGTH;
    if (out != NULL) {
      out[i].type = optType;
      out[i].name = buf + off;
      out[i].nameLen = nameLen;
    }
    off += nameLen;

    if (off > bufLen || (size_t)bufLen - off < valueLen) return WLR_DECODE_BAD_LENGTH;
    if (out != NULL) {
      out[i].value = (valueLen > 0) ? (buf + off) : NULL;
      out[i].valueLen = valueLen;
      out[i].hasValue = hasValue;
    }
    off += valueLen;
  }

  /* Every declared byte must be accounted for. Without this a record whose name/value
   * lengths under-describe the payload decodes as OK and the trailing bytes are ignored
   * -- so a corrupted entry table looks like a shorter but valid option list. This is the
   * same exact-consumption guarantee wlrDecodeBind enforces through its per-column sums. */
  if (off != bufLen) return WLR_DECODE_BAD_LENGTH;

  if (count != NULL) *count = (int32_t)n;
  return WLR_DECODE_OK;
}
