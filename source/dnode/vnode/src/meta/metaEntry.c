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

int meteEncodeColCmprEntry(SEncoder *pCoder, const SMetaEntry *pME) {
  const SColCmprWrapper *pw = &pME->colCmpr;
  TAOS_CHECK_RETURN(tEncodeI32v(pCoder, pw->nCols));
  TAOS_CHECK_RETURN(tEncodeI32v(pCoder, pw->version));
  uDebug("encode cols:%d", pw->nCols);

  for (int32_t i = 0; i < pw->nCols; i++) {
    SColCmpr *p = &pw->pColCmpr[i];
    TAOS_CHECK_RETURN(tEncodeI16v(pCoder, p->id));
    TAOS_CHECK_RETURN(tEncodeU32(pCoder, p->alg));
  }
  return 0;
}
int meteDecodeColCmprEntry(SDecoder *pDecoder, SMetaEntry *pME) {
  SColCmprWrapper *pWrapper = &pME->colCmpr;
  TAOS_CHECK_RETURN(tDecodeI32v(pDecoder, &pWrapper->nCols));
  if (pWrapper->nCols == 0) {
    return 0;
  }

  TAOS_CHECK_RETURN(tDecodeI32v(pDecoder, &pWrapper->version));
  uDebug("dencode cols:%d", pWrapper->nCols);
  pWrapper->pColCmpr = (SColCmpr *)tDecoderMalloc(pDecoder, pWrapper->nCols * sizeof(SColCmpr));
  if (pWrapper->pColCmpr == NULL) {
    return terrno;
  }

  for (int i = 0; i < pWrapper->nCols; i++) {
    SColCmpr *p = &pWrapper->pColCmpr[i];
    TAOS_CHECK_RETURN(tDecodeI16v(pDecoder, &p->id));
    TAOS_CHECK_RETURN(tDecodeU32(pDecoder, &p->alg));
  }
  return 0;
}
static FORCE_INLINE int32_t metatInitDefaultSColCmprWrapper(SDecoder *pDecoder, SColCmprWrapper *pCmpr,
                                                            SSchemaWrapper *pSchema) {
  pCmpr->nCols = pSchema->nCols;
  if ((pCmpr->pColCmpr = (SColCmpr *)tDecoderMalloc(pDecoder, pCmpr->nCols * sizeof(SColCmpr))) == NULL) {
    return terrno;
  }

  for (int32_t i = 0; i < pCmpr->nCols; i++) {
    SColCmpr *pColCmpr = &pCmpr->pColCmpr[i];
    SSchema  *pColSchema = &pSchema->pSchema[i];
    pColCmpr->id = pColSchema->colId;
    pColCmpr->alg = createDefaultColCmprByType(pColSchema->type);
  }
  return 0;
}

int metaEncodeEntry(SEncoder *pCoder, const SMetaEntry *pME) {
  TAOS_CHECK_RETURN(tStartEncode(pCoder));
  TAOS_CHECK_RETURN(tEncodeI64(pCoder, pME->version));
  TAOS_CHECK_RETURN(tEncodeI8(pCoder, pME->type));
  TAOS_CHECK_RETURN(tEncodeI64(pCoder, pME->uid));

  if (pME->name == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  TAOS_CHECK_RETURN(tEncodeCStr(pCoder, pME->name));

  if (pME->type == TSDB_SUPER_TABLE) {
    TAOS_CHECK_RETURN(tEncodeI8(pCoder, pME->flags));
    TAOS_CHECK_RETURN(tEncodeSSchemaWrapper(pCoder, &pME->stbEntry.schemaRow));
    TAOS_CHECK_RETURN(tEncodeSSchemaWrapper(pCoder, &pME->stbEntry.schemaTag));
    if (TABLE_IS_ROLLUP(pME->flags)) {
      TAOS_CHECK_RETURN(tEncodeSRSmaParam(pCoder, &pME->stbEntry.rsmaParam));
    }
  } else if (pME->type == TSDB_CHILD_TABLE) {
    TAOS_CHECK_RETURN(tEncodeI64(pCoder, pME->ctbEntry.btime));
    TAOS_CHECK_RETURN(tEncodeI32(pCoder, pME->ctbEntry.ttlDays));
    TAOS_CHECK_RETURN(tEncodeI32v(pCoder, pME->ctbEntry.commentLen));
    if (pME->ctbEntry.commentLen > 0) {
      TAOS_CHECK_RETURN(tEncodeCStr(pCoder, pME->ctbEntry.comment));
    }
    TAOS_CHECK_RETURN(tEncodeI64(pCoder, pME->ctbEntry.suid));
    TAOS_CHECK_RETURN(tEncodeTag(pCoder, (const STag *)pME->ctbEntry.pTags));
  } else if (pME->type == TSDB_NORMAL_TABLE) {
    TAOS_CHECK_RETURN(tEncodeI64(pCoder, pME->ntbEntry.btime));
    TAOS_CHECK_RETURN(tEncodeI32(pCoder, pME->ntbEntry.ttlDays));
    TAOS_CHECK_RETURN(tEncodeI32v(pCoder, pME->ntbEntry.commentLen));
    if (pME->ntbEntry.commentLen > 0) {
      TAOS_CHECK_RETURN(tEncodeCStr(pCoder, pME->ntbEntry.comment));
    }
    TAOS_CHECK_RETURN(tEncodeI32v(pCoder, pME->ntbEntry.ncid));
    TAOS_CHECK_RETURN(tEncodeSSchemaWrapper(pCoder, &pME->ntbEntry.schemaRow));
  } else if (pME->type == TSDB_TSMA_TABLE) {
    TAOS_CHECK_RETURN(tEncodeTSma(pCoder, pME->smaEntry.tsma));
  } else {
    metaError("meta/entry: invalide table type: %" PRId8 " encode failed.", pME->type);
    return TSDB_CODE_INVALID_PARA;
  }
  TAOS_CHECK_RETURN(meteEncodeColCmprEntry(pCoder, pME));

  tEndEncode(pCoder);
  return 0;
}

int metaDecodeEntry(SDecoder *pCoder, SMetaEntry *pME) {
  TAOS_CHECK_RETURN(tStartDecode(pCoder));
  TAOS_CHECK_RETURN(tDecodeI64(pCoder, &pME->version));
  TAOS_CHECK_RETURN(tDecodeI8(pCoder, &pME->type));
  TAOS_CHECK_RETURN(tDecodeI64(pCoder, &pME->uid));
  TAOS_CHECK_RETURN(tDecodeCStr(pCoder, &pME->name));

  if (pME->type == TSDB_SUPER_TABLE) {
    TAOS_CHECK_RETURN(tDecodeI8(pCoder, &pME->flags));
    TAOS_CHECK_RETURN(tDecodeSSchemaWrapperEx(pCoder, &pME->stbEntry.schemaRow));
    TAOS_CHECK_RETURN(tDecodeSSchemaWrapperEx(pCoder, &pME->stbEntry.schemaTag));
    if (TABLE_IS_ROLLUP(pME->flags)) {
      TAOS_CHECK_RETURN(tDecodeSRSmaParam(pCoder, &pME->stbEntry.rsmaParam));
    }
  } else if (pME->type == TSDB_CHILD_TABLE) {
    TAOS_CHECK_RETURN(tDecodeI64(pCoder, &pME->ctbEntry.btime));
    TAOS_CHECK_RETURN(tDecodeI32(pCoder, &pME->ctbEntry.ttlDays));
    TAOS_CHECK_RETURN(tDecodeI32v(pCoder, &pME->ctbEntry.commentLen));
    if (pME->ctbEntry.commentLen > 0) {
      TAOS_CHECK_RETURN(tDecodeCStr(pCoder, &pME->ctbEntry.comment));
    }
    TAOS_CHECK_RETURN(tDecodeI64(pCoder, &pME->ctbEntry.suid));
    TAOS_CHECK_RETURN(tDecodeTag(pCoder, (STag **)&pME->ctbEntry.pTags));
  } else if (pME->type == TSDB_NORMAL_TABLE) {
    TAOS_CHECK_RETURN(tDecodeI64(pCoder, &pME->ntbEntry.btime));
    TAOS_CHECK_RETURN(tDecodeI32(pCoder, &pME->ntbEntry.ttlDays));
    TAOS_CHECK_RETURN(tDecodeI32v(pCoder, &pME->ntbEntry.commentLen));
    if (pME->ntbEntry.commentLen > 0) {
      TAOS_CHECK_RETURN(tDecodeCStr(pCoder, &pME->ntbEntry.comment));
    }
    TAOS_CHECK_RETURN(tDecodeI32v(pCoder, &pME->ntbEntry.ncid));
    TAOS_CHECK_RETURN(tDecodeSSchemaWrapperEx(pCoder, &pME->ntbEntry.schemaRow));
  } else if (pME->type == TSDB_TSMA_TABLE) {
    pME->smaEntry.tsma = tDecoderMalloc(pCoder, sizeof(STSma));
    if (!pME->smaEntry.tsma) {
      return terrno;
    }
    TAOS_CHECK_RETURN(tDecodeTSma(pCoder, pME->smaEntry.tsma, true));
  } else {
    metaError("meta/entry: invalide table type: %" PRId8 " decode failed.", pME->type);
    return TSDB_CODE_INVALID_PARA;
  }
  if (pME->type == TSDB_SUPER_TABLE) {
    if (TABLE_IS_COL_COMPRESSED(pME->flags)) {
      TAOS_CHECK_RETURN(meteDecodeColCmprEntry(pCoder, pME));

      if (pME->colCmpr.nCols == 0) {
        TAOS_CHECK_RETURN(metatInitDefaultSColCmprWrapper(pCoder, &pME->colCmpr, &pME->stbEntry.schemaRow));
      }
    } else {
      TAOS_CHECK_RETURN(metatInitDefaultSColCmprWrapper(pCoder, &pME->colCmpr, &pME->stbEntry.schemaRow));
      TABLE_SET_COL_COMPRESSED(pME->flags);
    }
  } else if (pME->type == TSDB_NORMAL_TABLE) {
    if (!tDecodeIsEnd(pCoder)) {
      uDebug("set type: %d, tableName:%s", pME->type, pME->name);
      TAOS_CHECK_RETURN(meteDecodeColCmprEntry(pCoder, pME));
      if (pME->colCmpr.nCols == 0) {
        TAOS_CHECK_RETURN(metatInitDefaultSColCmprWrapper(pCoder, &pME->colCmpr, &pME->ntbEntry.schemaRow));
      }
    } else {
      uDebug("set default type: %d, tableName:%s", pME->type, pME->name);
      TAOS_CHECK_RETURN(metatInitDefaultSColCmprWrapper(pCoder, &pME->colCmpr, &pME->ntbEntry.schemaRow));
    }
    TABLE_SET_COL_COMPRESSED(pME->flags);
  }

  tEndDecode(pCoder);
  return 0;
}
