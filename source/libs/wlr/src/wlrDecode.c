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
 * WLR decoder (DS 6.15.2, 6.15.3).
 *
 * Two rules govern everything here.
 *
 * First, recording files are UNTRUSTED input (DS 8): they may have been copied
 * from a customer site, truncated mid-write, or tampered with. Every length is
 * bounds-checked against both the declared frame length and the actual buffer
 * before it is used, so a hostile length cannot walk off the end.
 *
 * Second, an incomplete TAIL is normal and an incomplete MIDDLE is not. Copying
 * an actively-written segment legitimately yields a half frame at the end
 * (FS 4.9); that is WLR_DECODE_TORN_TAIL and the caller keeps the valid prefix.
 * A CRC failure or length inconsistency in the middle of a file is real damage
 * and sets structurallyComplete=false. Collapsing these two into one error would
 * make every active-file copy look corrupt.
 */

#include "wlrFormat.h"

#include "tcrc32c.h"

EWlrDecodeResult wlrDecodeFileHeader(const uint8_t *buf, size_t bufLen, SWlrFileHeader *hdr) {
  uint16_t major;
  uint16_t minor;
  uint16_t endian;
  uint16_t headerLength;

  if (buf == NULL || hdr == NULL) return WLR_DECODE_BAD_ARG;
  /* Too short to even hold the fixed prefix: treat as a torn tail rather than
   * corruption, since a zero-length or partially-created file is what an
   * interrupted startup leaves behind. */
  if (bufLen < (size_t)WLR_FILE_HEADER_V0_SIZE) return WLR_DECODE_TORN_TAIL;

  if (memcmp(buf + WLR_HDR_OFF_MAGIC, WLR_FORMAT_MAGIC, WLR_FORMAT_MAGIC_LEN) != 0) {
    return WLR_DECODE_BAD_MAGIC;
  }

  major = wlrGetU16(buf + WLR_HDR_OFF_MAJOR);
  minor = wlrGetU16(buf + WLR_HDR_OFF_MINOR);
  endian = wlrGetU16(buf + WLR_HDR_OFF_ENDIAN);
  headerLength = wlrGetU16(buf + WLR_HDR_OFF_HDRLEN);

  if (!wlrVersionSupported(major, minor)) return WLR_DECODE_BAD_VERSION;

  /* A mismatched mark means the writer emitted host-endian bytes. Reject rather
   * than byte-swap: a conforming writer never produces this, so the file came
   * from something we cannot trust to be self-consistent. */
  if (endian != WLR_FORMAT_ENDIAN_MARK) return WLR_DECODE_BAD_ENDIAN;

  uint16_t minimumLength = minor >= 1 ? WLR_FILE_HEADER_SIZE : WLR_FILE_HEADER_V0_SIZE;
  if (headerLength < minimumLength || (size_t)headerLength > bufLen) {
    return WLR_DECODE_BAD_LENGTH;
  }
  if (minor >= 1 && memchr(buf + WLR_HDR_OFF_CLOCK_DOMAIN, 0, WLR_CLOCK_DOMAIN_ID_LEN) == NULL) {
    return WLR_DECODE_BAD_LENGTH;
  }

  memset(hdr, 0, sizeof(*hdr));
  hdr->major = major;
  hdr->minor = minor;
  hdr->headerLength = headerLength;
  memcpy(hdr->sessionId, buf + WLR_HDR_OFF_SESSION_ID, WLR_SESSION_ID_LEN);
  hdr->processStartWallNs = (int64_t)wlrGetU64(buf + WLR_HDR_OFF_START_WALL);
  hdr->processStartMonoNs = (int64_t)wlrGetU64(buf + WLR_HDR_OFF_START_MONO);
  hdr->pid = wlrGetU64(buf + WLR_HDR_OFF_PID);
  hdr->segmentIndex = wlrGetU32(buf + WLR_HDR_OFF_SEGMENT_IDX);
  hdr->firstEventSeq = wlrGetU64(buf + WLR_HDR_OFF_FIRST_SEQ);
  if (minor >= 1) {
    memcpy(hdr->clockDomainId, buf + WLR_HDR_OFF_CLOCK_DOMAIN, WLR_CLOCK_DOMAIN_ID_LEN);
  }

  return WLR_DECODE_OK;
}

EWlrDecodeResult wlrDecodeFrame(const uint8_t *buf, size_t bufLen, SWlrFrameHeader *hdr,
                                const uint8_t **payload, uint32_t *consumed) {
  uint32_t       frameLength;
  uint32_t       payloadLength;
  size_t         bodyLen;
  uint32_t       storedCrc;
  uint32_t       actualCrc;
  const uint8_t *body;

  if (buf == NULL || hdr == NULL || consumed == NULL) return WLR_DECODE_BAD_ARG;

  /* Not even the length prefix: the writer was interrupted here. */
  if (bufLen < sizeof(uint32_t)) return WLR_DECODE_TORN_TAIL;

  frameLength = wlrGetU32(buf + WLR_FRAME_OFF_LENGTH);

  /* Below the structural minimum the value is meaningless, so this is damage
   * rather than truncation -- a torn write would leave the prefix absent, not
   * present-but-nonsensical. */
  if (frameLength < (uint32_t)WLR_FRAME_MIN_LENGTH) return WLR_DECODE_BAD_LENGTH;

  /* Bound the payload before arithmetic, then separately guard frame-offset
   * arithmetic and check against what we actually hold. Short buffer means the
   * tail is torn. */
  if (frameLength - (uint32_t)WLR_FRAME_MIN_LENGTH > (uint32_t)WLR_MAX_FRAME_PAYLOAD) {
    return WLR_DECODE_BAD_LENGTH;
  }
  if ((size_t)frameLength > SIZE_MAX - (size_t)WLR_FRAME_OFF_TYPE ||
      (size_t)WLR_FRAME_OFF_TYPE + (size_t)frameLength > bufLen) {
    return WLR_DECODE_TORN_TAIL;
  }

  body = buf + WLR_FRAME_OFF_TYPE;
  bodyLen = (size_t)frameLength - (WLR_FRAME_MIN_LENGTH - WLR_FRAME_CRC_PREFIX_LEN);

  payloadLength = wlrGetU32(buf + WLR_FRAME_OFF_PAYLOAD_LEN);
  if (payloadLength > (uint32_t)WLR_MAX_FRAME_PAYLOAD) return WLR_DECODE_BAD_LENGTH;

  /* The independent payload bound keeps safety from depending on this equality's
   * position in the validation sequence. */
  if (bodyLen != (size_t)WLR_FRAME_CRC_PREFIX_LEN + (size_t)payloadLength) {
    return WLR_DECODE_BAD_LENGTH;
  }

  storedCrc = wlrGetU32(body + bodyLen);
  actualCrc = (*crc32c)(0, (crc_stream)body, bodyLen);
  if (storedCrc != actualCrc) return WLR_DECODE_CRC_MISMATCH;

  /* CRC passed, so the bytes are intact; a missing terminator therefore means
   * the writer had not finished this frame. Report it as unterminated rather
   * than corrupt so the caller keeps the prefix (DS 6.15.3). */
  if (buf[WLR_FRAME_OFF_TYPE + frameLength - 1] != WLR_FRAME_DELIMITER) return WLR_DECODE_MISSING_LF;

  hdr->type = wlrGetU16(buf + WLR_FRAME_OFF_TYPE);
  hdr->flags = wlrGetU16(buf + WLR_FRAME_OFF_FLAGS);
  hdr->eventSequence = wlrGetU64(buf + WLR_FRAME_OFF_EVENT_SEQ);
  hdr->logicalEventId = wlrGetU64(buf + WLR_FRAME_OFF_LOGICAL_ID);
  hdr->payloadLength = payloadLength;

  if (payload != NULL) {
    *payload = (payloadLength > 0) ? (buf + WLR_FRAME_OFF_PAYLOAD) : NULL;
  }
  *consumed = WLR_FRAME_OFF_TYPE + frameLength;

  return WLR_DECODE_OK;
}

EWlrDecodeResult wlrDecodeTlvs(const uint8_t *payload, uint32_t payloadLen, SWlrTlv *tlvs,
                               int32_t tlvCapacity, int32_t *tlvCount) {
  uint32_t off = 0;
  int32_t  n = 0;

  if (tlvCount != NULL) *tlvCount = 0;
  if (tlvCapacity < 0 || (tlvs == NULL && tlvCapacity != 0)) return WLR_DECODE_BAD_ARG;
  if (payloadLen == 0) return WLR_DECODE_OK;
  if (payload == NULL) return WLR_DECODE_BAD_ARG;

  while (off < payloadLen) {
    uint32_t len;

    /* Header must fit entirely within the remaining payload. */
    if (payloadLen - off < WLR_TLV_HEADER_SIZE) return WLR_DECODE_BAD_LENGTH;

    len = wlrGetU32(payload + off + WLR_TLV_OFF_LENGTH);
    off += WLR_TLV_HEADER_SIZE;

    /* Subtraction, never addition: off + len could overflow on a hostile len. */
    if (len > payloadLen - off) return WLR_DECODE_BAD_LENGTH;

    ++n;
    if (n > (int32_t)(WLR_MAX_FRAME_PAYLOAD / WLR_TLV_HEADER_SIZE)) return WLR_DECODE_BAD_LENGTH;
    off += len;
  }

  if (tlvCount != NULL) *tlvCount = n;
  if (tlvs == NULL) return WLR_DECODE_OK;
  if (n > tlvCapacity) return WLR_DECODE_NO_SPACE;

  /* Fill only after a complete validation/count pass. On NO_SPACE the caller needs
   * the required count, but returning a partially populated array makes accidental
   * use of that count especially hazardous. Keeping output transactional also matches
   * the other count-then-fill decoders. */
  off = 0;
  n = 0;
  while (off < payloadLen) {
    uint32_t len = wlrGetU32(payload + off + WLR_TLV_OFF_LENGTH);
    tlvs[n].tag = wlrGetU16(payload + off + WLR_TLV_OFF_TAG);
    tlvs[n].flags = wlrGetU16(payload + off + WLR_TLV_OFF_FLAGS);
    tlvs[n].length = len;
    off += WLR_TLV_HEADER_SIZE;
    tlvs[n].value = (len > 0) ? (payload + off) : NULL;
    ++n;
    off += len;
  }
  return WLR_DECODE_OK;
}
