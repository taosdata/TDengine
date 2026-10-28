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
 * WLR encoder: in-memory structs to on-disk bytes (DS 6.15.2).
 *
 * Every multi-byte integer goes out through the wlrPutU* helpers. Do not
 * replace them with memcpy of a native integer or with a packed struct: the
 * format is fixed little-endian on every platform, and struct padding is not
 * part of it.
 */

#include "wlrFormat.h"

#include "tcrc32c.h"

/* ------------------------------------------------------- little-endian I/O */

void wlrPutU16(uint8_t *buf, uint16_t v) {
  buf[0] = (uint8_t)(v & 0xFFu);
  buf[1] = (uint8_t)((v >> 8) & 0xFFu);
}

void wlrPutU32(uint8_t *buf, uint32_t v) {
  buf[0] = (uint8_t)(v & 0xFFu);
  buf[1] = (uint8_t)((v >> 8) & 0xFFu);
  buf[2] = (uint8_t)((v >> 16) & 0xFFu);
  buf[3] = (uint8_t)((v >> 24) & 0xFFu);
}

void wlrPutU64(uint8_t *buf, uint64_t v) {
  buf[0] = (uint8_t)(v & 0xFFu);
  buf[1] = (uint8_t)((v >> 8) & 0xFFu);
  buf[2] = (uint8_t)((v >> 16) & 0xFFu);
  buf[3] = (uint8_t)((v >> 24) & 0xFFu);
  buf[4] = (uint8_t)((v >> 32) & 0xFFu);
  buf[5] = (uint8_t)((v >> 40) & 0xFFu);
  buf[6] = (uint8_t)((v >> 48) & 0xFFu);
  buf[7] = (uint8_t)((v >> 56) & 0xFFu);
}

uint16_t wlrGetU16(const uint8_t *buf) { return (uint16_t)((uint16_t)buf[0] | ((uint16_t)buf[1] << 8)); }

uint32_t wlrGetU32(const uint8_t *buf) {
  return (uint32_t)buf[0] | ((uint32_t)buf[1] << 8) | ((uint32_t)buf[2] << 16) | ((uint32_t)buf[3] << 24);
}

uint64_t wlrGetU64(const uint8_t *buf) {
  return (uint64_t)buf[0] | ((uint64_t)buf[1] << 8) | ((uint64_t)buf[2] << 16) |
         ((uint64_t)buf[3] << 24) | ((uint64_t)buf[4] << 32) | ((uint64_t)buf[5] << 40) |
         ((uint64_t)buf[6] << 48) | ((uint64_t)buf[7] << 56);
}

/* ------------------------------------------------------------- file header */

int32_t wlrEncodeFileHeader(uint8_t *buf, size_t bufLen, const SWlrFileHeader *hdr) {
  if (buf == NULL || hdr == NULL) return -1;
  if (bufLen < (size_t)WLR_FILE_HEADER_SIZE) return -1;
  if (memchr(hdr->clockDomainId, 0, WLR_CLOCK_DOMAIN_ID_LEN) == NULL) return -1;

  memset(buf, 0, (size_t)WLR_FILE_HEADER_SIZE);
  memcpy(buf + WLR_HDR_OFF_MAGIC, WLR_FORMAT_MAGIC, WLR_FORMAT_MAGIC_LEN);
  wlrPutU16(buf + WLR_HDR_OFF_MAJOR, WLR_FORMAT_MAJOR);
  wlrPutU16(buf + WLR_HDR_OFF_MINOR, WLR_FORMAT_MINOR);
  wlrPutU16(buf + WLR_HDR_OFF_ENDIAN, WLR_FORMAT_ENDIAN_MARK);
  /* Recorded so a later minor version may append fields; readers skip to it
   * rather than assuming WLR_FILE_HEADER_SIZE. */
  wlrPutU16(buf + WLR_HDR_OFF_HDRLEN, (uint16_t)WLR_FILE_HEADER_SIZE);
  memcpy(buf + WLR_HDR_OFF_SESSION_ID, hdr->sessionId, WLR_SESSION_ID_LEN);
  wlrPutU64(buf + WLR_HDR_OFF_START_WALL, (uint64_t)(int64_t)hdr->processStartWallNs);
  wlrPutU64(buf + WLR_HDR_OFF_START_MONO, (uint64_t)(int64_t)hdr->processStartMonoNs);
  wlrPutU64(buf + WLR_HDR_OFF_PID, hdr->pid);
  wlrPutU32(buf + WLR_HDR_OFF_SEGMENT_IDX, hdr->segmentIndex);
  wlrPutU64(buf + WLR_HDR_OFF_FIRST_SEQ, hdr->firstEventSeq);
  memcpy(buf + WLR_HDR_OFF_CLOCK_DOMAIN, hdr->clockDomainId, strlen(hdr->clockDomainId));

  return WLR_FILE_HEADER_SIZE;
}

/* --------------------------------------------------------------------- TLV */

int32_t wlrEncodeTlv(uint8_t *buf, size_t bufLen, uint16_t tag, bool required, uint16_t flags,
                     const void *value, uint32_t valueLen) {
  size_t need;
  if (buf == NULL) return -1;
  /* A tag number must not collide with the required bit. */
  if ((tag & WLR_TAG_REQUIRED_BIT) != 0 || WLR_TAG_BASE(tag) == 0) return -1;
  if (!wlrTagIsKnown(tag)) return -1;
  if (valueLen > 0 && value == NULL) return -1;

  /* A TLV value must fit inside the 1 MiB frame payload cap. The header added below
   * is accounted for separately when bufLen is checked. */
  if (valueLen > WLR_MAX_FRAME_PAYLOAD) return -1;

  need = (size_t)WLR_TLV_HEADER_SIZE + (size_t)valueLen;
  if (bufLen < need) return -1;

  wlrPutU16(buf + WLR_TLV_OFF_TAG, (uint16_t)(required ? (tag | WLR_TAG_REQUIRED_BIT) : tag));
  wlrPutU16(buf + WLR_TLV_OFF_FLAGS, flags);
  wlrPutU32(buf + WLR_TLV_OFF_LENGTH, valueLen);
  if (valueLen > 0) {
    memcpy(buf + WLR_TLV_OFF_VALUE, value, (size_t)valueLen);
  }
  return (int32_t)need;
}

int32_t wlrEncodeTlvU64(uint8_t *buf, size_t bufLen, uint16_t tag, bool required, uint64_t v) {
  uint8_t tmp[8];
  wlrPutU64(tmp, v);
  return wlrEncodeTlv(buf, bufLen, tag, required, 0, tmp, (uint32_t)sizeof(tmp));
}

/* ------------------------------------------------------------------- frame */

size_t wlrFrameEncodedSize(uint32_t payloadLength) {
  /* Four-byte length, CRC-covered header prefix, payload, four-byte CRC, and LF. */
  return (size_t)4 + (size_t)WLR_FRAME_CRC_PREFIX_LEN + (size_t)payloadLength +
         (size_t)(WLR_FRAME_MIN_LENGTH - WLR_FRAME_CRC_PREFIX_LEN);
}

int32_t wlrEncodeFrame(uint8_t *buf, size_t bufLen, const SWlrFrameHeader *hdr, const uint8_t *payload) {
  size_t   total;
  size_t   bodyLen;
  uint32_t frameLength;
  uint32_t crc;

  if (buf == NULL || hdr == NULL) return -1;
  if (hdr->payloadLength > WLR_MAX_FRAME_PAYLOAD) return -1;
  if (hdr->payloadLength > 0 && payload == NULL) return -1;

  total = wlrFrameEncodedSize(hdr->payloadLength);
  if (bufLen < total) return -1;

  /* frameLength counts frameType through the trailing delimiter inclusive. */
  frameLength = (uint32_t)(total - sizeof(uint32_t));
  wlrPutU32(buf + WLR_FRAME_OFF_LENGTH, frameLength);

  wlrPutU16(buf + WLR_FRAME_OFF_TYPE, hdr->type);
  wlrPutU16(buf + WLR_FRAME_OFF_FLAGS, hdr->flags);
  wlrPutU64(buf + WLR_FRAME_OFF_EVENT_SEQ, hdr->eventSequence);
  wlrPutU64(buf + WLR_FRAME_OFF_LOGICAL_ID, hdr->logicalEventId);
  wlrPutU32(buf + WLR_FRAME_OFF_PAYLOAD_LEN, hdr->payloadLength);
  if (hdr->payloadLength > 0) {
    memcpy(buf + WLR_FRAME_OFF_PAYLOAD, payload, (size_t)hdr->payloadLength);
  }

  /* CRC covers frameType through the payloadLength field and then the payload;
   * it excludes frameLength, the CRC field itself, and the delimiter. */
  bodyLen = (size_t)WLR_FRAME_CRC_PREFIX_LEN + (size_t)hdr->payloadLength;
  crc = (*crc32c)(0, (crc_stream)(buf + WLR_FRAME_OFF_TYPE), bodyLen);
  wlrPutU32(buf + WLR_FRAME_OFF_PAYLOAD + hdr->payloadLength, crc);

  /* Trailing LF: purely so a pager shows record boundaries. Parsers never split
   * on it. The writer must treat body+CRC+LF as one unit, and Begin-Ack may only
   * be returned once this byte has been written (DS 6.15.2, 6.14). */
  buf[WLR_FRAME_OFF_PAYLOAD + hdr->payloadLength + WLR_FRAME_CRC_SIZE] = WLR_FRAME_DELIMITER;

  return (int32_t)total;
}

/* ---------------------------------------------------------------- chunking */

uint32_t wlrChunkCount(uint64_t totalLength) {
  if (totalLength == 0) return 0;
  if (totalLength > WLR_MAX_LOGICAL_PAYLOAD) return UINT32_MAX;
  return (uint32_t)(totalLength / WLR_CHUNK_SIZE +
                    ((totalLength % WLR_CHUNK_SIZE) != 0 ? 1 : 0));
}

bool wlrNeedsChunking(uint64_t totalLength) {
  return totalLength > (uint64_t)WLR_MAX_FRAME_PAYLOAD && totalLength <= WLR_MAX_LOGICAL_PAYLOAD;
}

bool wlrVersionSupported(uint16_t major, uint16_t minor) {
  /* Major must match exactly. Any minor is readable: a minor bump may only add
   * optional tags or new frame types, both of which older readers skip. */
  (void)minor;
  return major == WLR_FORMAT_MAJOR;
}
