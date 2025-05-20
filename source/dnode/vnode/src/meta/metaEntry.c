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

int32_t metaEncodeColEntryptionSubEntry(SEncoder *pCoder, STableEncryption *pEntryption) {
  int32_t len = 0;

  len = tEncodeI32(pCoder, pEntryption->tableType);
  len = tEncodeI64(pCoder, pEntryption->tuid);
  len = tEncodeI64(pCoder, pEntryption->tsuid);
  len = tEncodeI32(pCoder, pEntryption->fieldId);
  len = tEncodeI32(pCoder, pEntryption->serailId);
  len = tEncodeI32(pCoder, pEntryption->encryptionLen);
  len = tEncodeCStr(pCoder, pEntryption->encryptionKey);
  len = tEncodeI32(pCoder, pEntryption->decryptionLen);
  len = tEncodeCStr(pCoder, pEntryption->decryptionKey);

  return 0;
}
int32_t metaDecodeColEntryptionSubEntry(SDecoder *pCoder, SMetaEntry *pME) {
  int32_t code = 0;
  return 0;
}
int32_t metaEncodeColEntryptionEntry(SEncoder *pCoder, const SMetaEntry *pME) {
  int32_t code = 0;

  int32_t sz = taosArrayGetSize(pME->pEntryptionList);
  if (tEncodeI32v(pCoder, sz) < 0) return -1;

  for (int32_t i = 0; i < taosArrayGetSize(pME->pEntryptionList); i++) {
    STableEncryption *pEntryption = taosArrayGet(pME->pEntryptionList, i);
  }

  return code;
}

int32_t metaDecodeColEntryptionEntry(SDecoder *pCoder, SMetaEntry *pME) {
  int32_t code = 0;
  int32_t sz = 0;
  if (tDecodeI32(pCoder, &sz) < 0) {
    return -1;
  }
  pME->pEntryptionList = taosArrayInit(sz, sizeof(STableEncryption));
  if (pME->pEntryptionList == NULL) {
    return -1;
  }

  for (int32_t i = 0; i < sz; i++) {
    STableEncryption entryption = {0};

    taosArrayPush(pME->pEntryptionList, &entryption);
  }

  return code;
}
int meteEncodeColCmprEntry(SEncoder *pCoder, const SMetaEntry *pME) {
  const SColCmprWrapper *pw = &pME->colCmpr;
  if (tEncodeI32v(pCoder, pw->nCols) < 0) return -1;
  if (tEncodeI32v(pCoder, pw->version) < 0) return -1;
  uDebug("encode cols:%d", pw->nCols);

  for (int32_t i = 0; i < pw->nCols; i++) {
    SColCmpr *p = &pw->pColCmpr[i];
    if (tEncodeI16v(pCoder, p->id) < 0) return -1;
    if (tEncodeU32(pCoder, p->alg) < 0) return -1;
  }
  return 0;
}
int meteDecodeColCmprEntry(SDecoder *pDecoder, SMetaEntry *pME) {
  SColCmprWrapper *pWrapper = &pME->colCmpr;
  if (tDecodeI32v(pDecoder, &pWrapper->nCols) < 0) return -1;
  if (pWrapper->nCols == 0) {
    return 0;
  }

  if (tDecodeI32v(pDecoder, &pWrapper->version) < 0) return -1;
  uDebug("dencode cols:%d", pWrapper->nCols);
  pWrapper->pColCmpr = (SColCmpr *)tDecoderMalloc(pDecoder, pWrapper->nCols * sizeof(SColCmpr));
  if (pWrapper->pColCmpr == NULL) return -1;

  for (int i = 0; i < pWrapper->nCols; i++) {
    SColCmpr *p = &pWrapper->pColCmpr[i];
    if (tDecodeI16v(pDecoder, &p->id) < 0) return -1;
    if (tDecodeU32(pDecoder, &p->alg) < 0) return -1;
  }
  return 0;
}
static FORCE_INLINE void metatInitDefaultSColCmprWrapper(SDecoder *pDecoder, SColCmprWrapper *pCmpr,
                                                         SSchemaWrapper *pSchema) {
  pCmpr->nCols = pSchema->nCols;
  pCmpr->pColCmpr = (SColCmpr *)tDecoderMalloc(pDecoder, pCmpr->nCols * sizeof(SColCmpr));
  for (int32_t i = 0; i < pCmpr->nCols; i++) {
    SColCmpr *pColCmpr = &pCmpr->pColCmpr[i];
    SSchema  *pColSchema = &pSchema->pSchema[i];
    pColCmpr->id = pColSchema->colId;
    pColCmpr->alg = createDefaultColCmprByType(pColSchema->type);
  }
}

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
  if (meteEncodeColCmprEntry(pCoder, pME) < 0) return -1;

  if (metaEncodeColEntryptionEntry(pCoder, pME) < 0) return -1;

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
  if (pME->type == TSDB_SUPER_TABLE) {
    if (TABLE_IS_COL_COMPRESSED(pME->flags)) {
      if (meteDecodeColCmprEntry(pCoder, pME) < 0) return -1;

      if (pME->colCmpr.nCols == 0) {
        metatInitDefaultSColCmprWrapper(pCoder, &pME->colCmpr, &pME->stbEntry.schemaRow);
      }
    } else {
      metatInitDefaultSColCmprWrapper(pCoder, &pME->colCmpr, &pME->stbEntry.schemaRow);
      TABLE_SET_COL_COMPRESSED(pME->flags);
    }

    if (!tDecodeIsEnd(pCoder)) {
      if (metaDecodeColEntryptionEntry(pCoder, pME) < 0) {
        return -1;
      }
    }
  } else if (pME->type == TSDB_NORMAL_TABLE) {
    if (!tDecodeIsEnd(pCoder)) {
      uDebug("set type: %d, tableName:%s", pME->type, pME->name);
      if (meteDecodeColCmprEntry(pCoder, pME) < 0) return -1;
      if (pME->colCmpr.nCols == 0) {
        metatInitDefaultSColCmprWrapper(pCoder, &pME->colCmpr, &pME->ntbEntry.schemaRow);
      }
    } else {
      uDebug("set default type: %d, tableName:%s", pME->type, pME->name);
      metatInitDefaultSColCmprWrapper(pCoder, &pME->colCmpr, &pME->ntbEntry.schemaRow);
    }
    TABLE_SET_COL_COMPRESSED(pME->flags);
  }

  tEndDecode(pCoder);
  return 0;
}
