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

#include "meta.h"

int metaEncodeEntry(SEncoder *pCoder, const SMetaEntry *pME) {
  if (tStartEncode(pCoder) < 0) return -1;

  if (tEncodeI64(pCoder, pME->version) < 0) return -1;
  if (tEncodeI8(pCoder, pME->type) < 0) return -1;
  if (tEncodeI64(pCoder, pME->uid) < 0) return -1;
  if (pME->name == NULL || tEncodeCStr(pCoder, pME->name) < 0) return -1;

  if (pME->type == TSDB_SUPER_TABLE) {
    if (tEncodeI8(pCoder, pME->flags) < 0) return -1;  // TODO: need refactor?
    if (tEncodeSSchemaWrapper(pCoder, &pME->stbEntry.schemaRow) < 0) return -1;
    if (tEncodeSSchemaWrapper(pCoder, &pME->stbEntry.schemaTag) < 0) return -1;
    if (TABLE_IS_ROLLUP(pME->flags)) {
      if (tEncodeSRSmaParam(pCoder, &pME->stbEntry.rsmaParam) < 0) return -1;
    }
  } else if (pME->type == TSDB_CHILD_TABLE) {
    if (tEncodeI64(pCoder, pME->ctbEntry.btime) < 0) return -1;
    if (tEncodeI32(pCoder, pME->ctbEntry.ttlDays) < 0) return -1;
    if (tEncodeI32v(pCoder, pME->ctbEntry.commentLen) < 0) return -1;
    if (pME->ctbEntry.commentLen > 0) {
      if (tEncodeCStr(pCoder, pME->ctbEntry.comment) < 0) return -1;
    }
    if (tEncodeI64(pCoder, pME->ctbEntry.suid) < 0) return -1;
    if (tEncodeTag(pCoder, (const STag *)pME->ctbEntry.pTags) < 0) return -1;
  } else if (pME->type == TSDB_NORMAL_TABLE) {
    if (tEncodeI64(pCoder, pME->ntbEntry.btime) < 0) return -1;
    if (tEncodeI32(pCoder, pME->ntbEntry.ttlDays) < 0) return -1;
    if (tEncodeI32v(pCoder, pME->ntbEntry.commentLen) < 0) return -1;
    if (pME->ntbEntry.commentLen > 0) {
      if (tEncodeCStr(pCoder, pME->ntbEntry.comment) < 0) return -1;
    }
    if (tEncodeI32v(pCoder, pME->ntbEntry.ncid) < 0) return -1;
    if (tEncodeSSchemaWrapper(pCoder, &pME->ntbEntry.schemaRow) < 0) return -1;
  } else if (pME->type == TSDB_TSMA_TABLE) {
    if (tEncodeTSma(pCoder, pME->smaEntry.tsma) < 0) return -1;
  } else {
    metaError("meta/entry: invalide table type: %" PRId8 " encode failed.", pME->type);

    return -1;
  }

  tEndEncode(pCoder);
  return 0;
}

int metaDecodeEntry(SDecoder *pCoder, SMetaEntry *pME) {
  if (tStartDecode(pCoder) < 0) return -1;

  if (tDecodeI64(pCoder, &pME->version) < 0) return -1;
  if (tDecodeI8(pCoder, &pME->type) < 0) return -1;
  if (tDecodeI64(pCoder, &pME->uid) < 0) return -1;
  if (tDecodeCStr(pCoder, &pME->name) < 0) return -1;

  if (pME->type == TSDB_SUPER_TABLE) {
    if (tDecodeI8(pCoder, &pME->flags) < 0) return -1;  // TODO: need refactor?
    if (tDecodeSSchemaWrapperEx(pCoder, &pME->stbEntry.schemaRow) < 0) return -1;
    if (tDecodeSSchemaWrapperEx(pCoder, &pME->stbEntry.schemaTag) < 0) return -1;
    if (TABLE_IS_ROLLUP(pME->flags)) {
      if (tDecodeSRSmaParam(pCoder, &pME->stbEntry.rsmaParam) < 0) return -1;
    }
  } else if (pME->type == TSDB_CHILD_TABLE) {
    if (tDecodeI64(pCoder, &pME->ctbEntry.btime) < 0) return -1;
    if (tDecodeI32(pCoder, &pME->ctbEntry.ttlDays) < 0) return -1;
    if (tDecodeI32v(pCoder, &pME->ctbEntry.commentLen) < 0) return -1;
    if (pME->ctbEntry.commentLen > 0) {
      if (tDecodeCStr(pCoder, &pME->ctbEntry.comment) < 0) return -1;
    }
    if (tDecodeI64(pCoder, &pME->ctbEntry.suid) < 0) return -1;
    if (tDecodeTag(pCoder, (STag **)&pME->ctbEntry.pTags) < 0) return -1;  // (TODO)
  } else if (pME->type == TSDB_NORMAL_TABLE) {
    if (tDecodeI64(pCoder, &pME->ntbEntry.btime) < 0) return -1;
    if (tDecodeI32(pCoder, &pME->ntbEntry.ttlDays) < 0) return -1;
    if (tDecodeI32v(pCoder, &pME->ntbEntry.commentLen) < 0) return -1;
    if (pME->ntbEntry.commentLen > 0) {
      if (tDecodeCStr(pCoder, &pME->ntbEntry.comment) < 0) return -1;
    }
    if (tDecodeI32v(pCoder, &pME->ntbEntry.ncid) < 0) return -1;
    if (tDecodeSSchemaWrapperEx(pCoder, &pME->ntbEntry.schemaRow) < 0) return -1;
  } else if (pME->type == TSDB_TSMA_TABLE) {
    pME->smaEntry.tsma = tDecoderMalloc(pCoder, sizeof(STSma));
    if (!pME->smaEntry.tsma) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
    if (tDecodeTSma(pCoder, pME->smaEntry.tsma, true) < 0) return -1;
  } else {
    metaError("meta/entry: invalide table type: %" PRId8 " decode failed.", pME->type);

    return -1;
  }

  tEndDecode(pCoder);
  return 0;
}
