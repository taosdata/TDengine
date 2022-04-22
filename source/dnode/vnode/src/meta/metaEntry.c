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

#include "vnodeInt.h"

int metaEncodeEntry(SCoder *pCoder, const SMetaEntry *pME) {
  if (tStartEncode(pCoder) < 0) return -1;

  if (tEncodeI8(pCoder, pME->type) < 0) return -1;
  if (tEncodeI64(pCoder, pME->uid) < 0) return -1;
  if (tEncodeCStr(pCoder, pME->name) < 0) return -1;

  if (pME->type == TSDB_SUPER_TABLE) {
    if (tEncodeI16v(pCoder, pME->stbEntry.nCols) < 0) return -1;
    if (tEncodeI16v(pCoder, pME->stbEntry.sver) < 0) return -1;
    for (int iCol = 0; iCol < pME->stbEntry.nCols; iCol++) {
      if (tEncodeSSchema(pCoder, pME->stbEntry.pSchema + iCol) < 0) return -1;
    }

    if (tEncodeI16v(pCoder, pME->stbEntry.nTags) < 0) return -1;
    for (int iTag = 0; iTag < pME->stbEntry.nTags; iTag++) {
      if (tEncodeSSchema(pCoder, pME->stbEntry.pSchemaTg + iTag) < 0) return -1;
    }
  } else if (pME->type == TSDB_CHILD_TABLE) {
    if (tEncodeI64(pCoder, pME->ctbEntry.ctime) < 0) return -1;
    if (tEncodeI32(pCoder, pME->ctbEntry.ttlDays) < 0) return -1;
    if (tEncodeI64(pCoder, pME->ctbEntry.suid) < 0) return -1;
    if (tEncodeBinary(pCoder, pME->ctbEntry.pTags, kvRowLen(pME->ctbEntry.pTags)) < 0) return -1;
  } else if (pME->type == TSDB_NORMAL_TABLE) {
    if (tEncodeI64(pCoder, pME->ntbEntry.ctime) < 0) return -1;
    if (tEncodeI32(pCoder, pME->ntbEntry.ttlDays) < 0) return -1;
    if (tEncodeI16v(pCoder, pME->ntbEntry.nCols) < 0) return -1;
    if (tEncodeI16v(pCoder, pME->ntbEntry.sver) < 0) return -1;
    for (int iCol = 0; iCol < pME->ntbEntry.nCols; iCol++) {
      if (tEncodeSSchema(pCoder, pME->ntbEntry.pSchema + iCol) < 0) return -1;
    }
  } else {
    ASSERT(0);
  }

  tEndEncode(pCoder);
  return 0;
}

int metaDecodeEntry(SCoder *pCoder, SMetaEntry *pME) {
  if (tStartDecode(pCoder) < 0) return -1;

  if (tDecodeI8(pCoder, &pME->type) < 0) return -1;
  if (tDecodeI64(pCoder, &pME->uid) < 0) return -1;
  if (tDecodeCStr(pCoder, &pME->name) < 0) return -1;

  if (pME->type == TSDB_SUPER_TABLE) {
    if (tDecodeI16v(pCoder, &pME->stbEntry.nCols) < 0) return -1;
    if (tDecodeI16v(pCoder, &pME->stbEntry.sver) < 0) return -1;
    pME->stbEntry.pSchema = (SSchema *)TCODER_MALLOC(pCoder, sizeof(SSchema) * pME->stbEntry.nCols);
    if (pME->stbEntry.pSchema == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
    for (int iCol = 0; iCol < pME->stbEntry.nCols; iCol++) {
      if (tDecodeSSchema(pCoder, pME->stbEntry.pSchema + iCol) < 0) return -1;
    }

    if (tDecodeI16v(pCoder, &pME->stbEntry.nTags) < 0) return -1;
    pME->stbEntry.pSchemaTg = (SSchema *)TCODER_MALLOC(pCoder, sizeof(SSchema) * pME->stbEntry.nTags);
    if (pME->stbEntry.pSchemaTg == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
    for (int iTag = 0; iTag < pME->stbEntry.nTags; iTag++) {
      if (tDecodeSSchema(pCoder, pME->stbEntry.pSchemaTg + iTag) < 0) return -1;
    }
  } else if (pME->type == TSDB_CHILD_TABLE) {
    if (tDecodeI64(pCoder, &pME->ctbEntry.ctime) < 0) return -1;
    if (tDecodeI32(pCoder, &pME->ctbEntry.ttlDays) < 0) return -1;
    if (tDecodeI64(pCoder, &pME->ctbEntry.suid) < 0) return -1;
    if (tDecodeBinary(pCoder, &pME->ctbEntry.pTags, NULL) < 0) return -1;  // (TODO)
  } else if (pME->type == TSDB_NORMAL_TABLE) {
    if (tDecodeI64(pCoder, &pME->ntbEntry.ctime) < 0) return -1;
    if (tDecodeI32(pCoder, &pME->ntbEntry.ttlDays) < 0) return -1;
    if (tDecodeI16v(pCoder, &pME->ntbEntry.nCols) < 0) return -1;
    if (tDecodeI16v(pCoder, &pME->ntbEntry.sver) < 0) return -1;
    pME->ntbEntry.pSchema = (SSchema *)TCODER_MALLOC(pCoder, sizeof(SSchema) * pME->ntbEntry.nCols);
    if (pME->ntbEntry.pSchema == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
    for (int iCol = 0; iCol < pME->ntbEntry.nCols; iCol++) {
      if (tEncodeSSchema(pCoder, pME->ntbEntry.pSchema + iCol) < 0) return -1;
    }
  } else {
    ASSERT(0);
  }

  tEndDecode(pCoder);
  return 0;
}
