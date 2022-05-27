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
  if (tEncodeCStr(pCoder, pME->name) < 0) return -1;

  if (pME->type == TSDB_SUPER_TABLE) {
    if (tEncodeSSchemaWrapper(pCoder, &pME->stbEntry.schemaRow) < 0) return -1;
    if (tEncodeSSchemaWrapper(pCoder, &pME->stbEntry.schemaTag) < 0) return -1;
  } else if (pME->type == TSDB_CHILD_TABLE) {
    if (tEncodeI64(pCoder, pME->ctbEntry.ctime) < 0) return -1;
    if (tEncodeI32(pCoder, pME->ctbEntry.ttlDays) < 0) return -1;
    if (tEncodeI64(pCoder, pME->ctbEntry.suid) < 0) return -1;
    if (tEncodeBinary(pCoder, pME->ctbEntry.pTags, kvRowLen(pME->ctbEntry.pTags)) < 0) return -1;
  } else if (pME->type == TSDB_NORMAL_TABLE) {
    if (tEncodeI64(pCoder, pME->ntbEntry.ctime) < 0) return -1;
    if (tEncodeI32(pCoder, pME->ntbEntry.ttlDays) < 0) return -1;
    if (tEncodeI32v(pCoder, pME->ntbEntry.ncid) < 0) return -1;
    if (tEncodeSSchemaWrapper(pCoder, &pME->ntbEntry.schemaRow) < 0) return -1;
  } else if (pME->type == TSDB_TSMA_TABLE) {
    if (tEncodeTSma(pCoder, pME->smaEntry.tsma) < 0) return -1;
  } else {
    ASSERT(0);
  }

  tEndEncode(pCoder);
  return 0;
}

int metaDecodeEntry(SDecoder *pCoder, SMetaEntry *pME) {
  uint32_t len;
  if (tStartDecode(pCoder) < 0) return -1;

  if (tDecodeI64(pCoder, &pME->version) < 0) return -1;
  if (tDecodeI8(pCoder, &pME->type) < 0) return -1;
  if (tDecodeI64(pCoder, &pME->uid) < 0) return -1;
  if (tDecodeCStr(pCoder, &pME->name) < 0) return -1;

  if (pME->type == TSDB_SUPER_TABLE) {
    if (tDecodeSSchemaWrapperEx(pCoder, &pME->stbEntry.schemaRow) < 0) return -1;
    if (tDecodeSSchemaWrapperEx(pCoder, &pME->stbEntry.schemaTag) < 0) return -1;
  } else if (pME->type == TSDB_CHILD_TABLE) {
    if (tDecodeI64(pCoder, &pME->ctbEntry.ctime) < 0) return -1;
    if (tDecodeI32(pCoder, &pME->ctbEntry.ttlDays) < 0) return -1;
    if (tDecodeI64(pCoder, &pME->ctbEntry.suid) < 0) return -1;
    if (tDecodeBinary(pCoder, &pME->ctbEntry.pTags, &len) < 0) return -1;  // (TODO)
  } else if (pME->type == TSDB_NORMAL_TABLE) {
    if (tDecodeI64(pCoder, &pME->ntbEntry.ctime) < 0) return -1;
    if (tDecodeI32(pCoder, &pME->ntbEntry.ttlDays) < 0) return -1;
    if (tDecodeI32v(pCoder, &pME->ntbEntry.ncid) < 0) return -1;
    if (tDecodeSSchemaWrapperEx(pCoder, &pME->ntbEntry.schemaRow) < 0) return -1;
  } else if (pME->type == TSDB_TSMA_TABLE) {
    pME->smaEntry.tsma = tDecoderMalloc(pCoder, sizeof(STSma));
    if (!pME->smaEntry.tsma) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
    if (tDecodeTSma(pCoder, pME->smaEntry.tsma) < 0) return -1;
  } else {
    ASSERT(0);
  }

  tEndDecode(pCoder);
  return 0;
}
