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

#include "tsdb.h"
#include "tsdbFS2.h"

#define TSDB_SNAP_MSG_VER 1

// tsdb replication opts
static int32_t tTsdbRepOptsDataLenCalc(STsdbRepOpts* pInfo) {
  int32_t hdrLen = sizeof(int32_t);
  int32_t datLen = 0;

  int8_t  msgVer = 0;
  int64_t reserved64 = 0;
  int16_t format = 0;
  hdrLen += sizeof(msgVer);
  datLen += hdrLen;
  datLen += sizeof(format);
  datLen += sizeof(reserved64);
  datLen += sizeof(*pInfo);
  return datLen;
}

int32_t tSerializeTsdbRepOpts(void* buf, int32_t bufLen, STsdbRepOpts* pOpts) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  int64_t reserved64 = 0;
  int8_t  msgVer = TSDB_SNAP_MSG_VER;

  if (tStartEncode(&encoder) < 0) goto _err;
  if (tEncodeI8(&encoder, msgVer) < 0) goto _err;
  int16_t format = pOpts->format;
  if (tEncodeI16(&encoder, format) < 0) goto _err;
  if (tEncodeI64(&encoder, reserved64) < 0) goto _err;

  tEndEncode(&encoder);
  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;

_err:
  tEncoderClear(&encoder);
  return -1;
}

int32_t tDeserializeTsdbRepOpts(void* buf, int32_t bufLen, STsdbRepOpts* pOpts) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  int64_t reserved64 = 0;
  int8_t  msgVer = 0;

  if (tStartDecode(&decoder) < 0) goto _err;
  if (tDecodeI8(&decoder, &msgVer) < 0) goto _err;
  if (msgVer != TSDB_SNAP_MSG_VER) goto _err;
  int16_t format = 0;
  if (tDecodeI16(&decoder, &format) < 0) goto _err;
  pOpts->format = format;
  if (tDecodeI64(&decoder, &reserved64) < 0) goto _err;

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;

_err:
  tDecoderClear(&decoder);
  return -1;
}

static int32_t tsdbRepOptsEstSize(STsdbRepOpts* pOpts) {
  int32_t dataLen = 0;
  dataLen += sizeof(SSyncTLV);
  dataLen += tTsdbRepOptsDataLenCalc(pOpts);
  return dataLen;
}

static int32_t tsdbRepOptsSerialize(STsdbRepOpts* pOpts, void* buf, int32_t bufLen) {
  SSyncTLV* pSubHead = buf;
  int32_t   offset = 0;
  int32_t   tlen = 0;
  if ((tlen = tSerializeTsdbRepOpts(pSubHead->val, bufLen, pOpts)) < 0) {
    return -1;
  }
  pSubHead->typ = SNAP_DATA_RAW;
  pSubHead->len = tlen;
  offset += sizeof(*pSubHead) + tlen;
  return offset;
}

// snap info
static int32_t tsdbSnapPrepDealWithSnapInfo(SVnode* pVnode, SSnapshot* pSnap, STsdbRepOpts* pInfo) {
  if (!pSnap->data) return 0;
  int32_t code = -1;

  SSyncTLV* pHead = (void*)pSnap->data;
  int32_t   offset = 0;

  while (offset + sizeof(*pHead) < pHead->len) {
    SSyncTLV* pField = (void*)(pHead->val + offset);
    offset += sizeof(*pField) + pField->len;
    void*   buf = pField->val;
    int32_t bufLen = pField->len;

    switch (pField->typ) {
      case SNAP_DATA_TSDB:
      case SNAP_DATA_RSMA1:
      case SNAP_DATA_RSMA2: {
      } break;
      case SNAP_DATA_RAW: {
        if (tDeserializeTsdbRepOpts(buf, bufLen, pInfo) < 0) {
          terrno = TSDB_CODE_INVALID_DATA_FMT;
          tsdbError("vgId:%d, failed to deserialize tsdb rep opts since %s", TD_VID(pVnode), terrstr());
          goto _out;
        }
      } break;
      default:
        tsdbError("vgId:%d, unexpected subfield type of snap info. typ:%d", TD_VID(pVnode), pField->typ);
        goto _out;
    }
  }

  code = 0;
_out:
  return code;
}

int32_t tsdbSnapPrepDescription(SVnode* pVnode, SSnapshot* pSnap) {
  ASSERT(pSnap->type == TDMT_SYNC_PREP_SNAPSHOT || pSnap->type == TDMT_SYNC_PREP_SNAPSHOT_REPLY);
  int code = -1;

  // deal with snap info for reply
  STsdbRepOpts opts = {.format = TSDB_SNAP_REP_FMT_RAW};
  if (pSnap->type == TDMT_SYNC_PREP_SNAPSHOT_REPLY) {
    STsdbRepOpts leaderOpts = {0};
    if (tsdbSnapPrepDealWithSnapInfo(pVnode, pSnap, &leaderOpts) < 0) {
      tsdbError("vgId:%d, failed to deal with snap info for reply since %s", TD_VID(pVnode), terrstr());
      goto _out;
    }
    opts.format = TMIN(opts.format, leaderOpts.format);
  }

  // info data realloc
  const int32_t headLen = sizeof(SSyncTLV);
  int32_t       bufLen = headLen;
  bufLen += tsdbRepOptsEstSize(&opts);
  if (syncSnapInfoDataRealloc(pSnap, bufLen) != 0) {
    tsdbError("vgId:%d, failed to realloc memory for data of snapshot info. bytes:%d", TD_VID(pVnode), bufLen);
    goto _out;
  }

  // serialization
  char*   buf = (void*)pSnap->data;
  int32_t offset = headLen;
  int32_t tlen = 0;

  if ((tlen = tsdbRepOptsSerialize(&opts, buf + offset, bufLen - offset)) < 0) {
    tsdbError("vgId:%d, failed to serialize tsdb rep opts since %s", TD_VID(pVnode), terrstr());
    goto _out;
  }
  offset += tlen;
  ASSERT(offset <= bufLen);

  // set header of info data
  SSyncTLV* pHead = pSnap->data;
  pHead->typ = pSnap->type;
  pHead->len = offset - headLen;

  tsdbInfo("vgId:%d, tsdb snap info prepared. type:%s, val length:%d", TD_VID(pVnode), TMSG_INFO(pHead->typ),
           pHead->len);
  code = 0;
_out:
  return code;
}
