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

static bool schemasHasTypeMod(const SSchema *pSchema, int32_t nCols) {
  for (int32_t i = 0; i < nCols; i++) {
    if (HAS_TYPE_MOD(pSchema + i)) {
      return true;
    }
  }
  return false;
}

static int32_t metaEncodeExtSchema(SEncoder* pCoder, const SMetaEntry* pME) {
  if (pME->pExtSchemas) {
    const SSchemaWrapper *pSchWrapper = NULL;
    bool                  hasTypeMods = false;
    if (pME->type == TSDB_SUPER_TABLE) {
      pSchWrapper = &pME->stbEntry.schemaRow;
    } else if (pME->type == TSDB_NORMAL_TABLE) {
      pSchWrapper = &pME->ntbEntry.schemaRow;
    } else {
      return 0;
    }
    hasTypeMods = schemasHasTypeMod(pSchWrapper->pSchema, pSchWrapper->nCols);

    for (int32_t i = 0; i < pSchWrapper->nCols && hasTypeMods; ++i) {
      TAOS_CHECK_RETURN(tEncodeI32v(pCoder, pME->pExtSchemas[i].typeMod));
    }
  }
  return 0;
}

static int32_t metaDecodeExtSchemas(SDecoder* pDecoder, SMetaEntry* pME) {
  bool hasExtSchema = false;
  SSchemaWrapper* pSchWrapper = NULL;
  if (pME->type == TSDB_SUPER_TABLE) {
    pSchWrapper = &pME->stbEntry.schemaRow;
  } else if (pME->type == TSDB_NORMAL_TABLE) {
    pSchWrapper = &pME->ntbEntry.schemaRow;
  } else {
    return 0;
  }

  hasExtSchema = schemasHasTypeMod(pSchWrapper->pSchema, pSchWrapper->nCols);
  if (hasExtSchema && pSchWrapper->nCols > 0) {
    pME->pExtSchemas = (SExtSchema*)tDecoderMalloc(pDecoder, sizeof(SExtSchema) * pSchWrapper->nCols);
    if (pME->pExtSchemas == NULL) {
      return terrno;
    }

    for (int32_t i = 0; i < pSchWrapper->nCols && hasExtSchema; i++) {
      TAOS_CHECK_RETURN(tDecodeI32v(pDecoder, &pME->pExtSchemas[i].typeMod));
    }
  }

  return 0;
}

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

static int32_t metaCloneColCmpr(const SColCmprWrapper *pSrc, SColCmprWrapper *pDst) {
  if (pSrc->nCols > 0) {
    pDst->nCols = pSrc->nCols;
    pDst->version = pSrc->version;
    pDst->pColCmpr = (SColCmpr *)taosMemoryCalloc(pSrc->nCols, sizeof(SColCmpr));
    if (NULL == pDst->pColCmpr) {
      return terrno;
    }
    memcpy(pDst->pColCmpr, pSrc->pColCmpr, pSrc->nCols * sizeof(SColCmpr));
  }
  return 0;
}

static void metaCloneColCmprFree(SColCmprWrapper *pCmpr) {
  if (pCmpr) {
    taosMemoryFreeClear(pCmpr->pColCmpr);
  }
}

int metaEncodeEntry(SEncoder *pCoder, const SMetaEntry *pME) {
  TAOS_CHECK_RETURN(tStartEncode(pCoder));
  TAOS_CHECK_RETURN(tEncodeI64(pCoder, pME->version));
  TAOS_CHECK_RETURN(tEncodeI8(pCoder, pME->type));
  TAOS_CHECK_RETURN(tEncodeI64(pCoder, pME->uid));

  if (pME->type > 0) {
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
    TAOS_CHECK_RETURN(metaEncodeExtSchema(pCoder, pME));
  }

  tEndEncode(pCoder);
  return 0;
}

int metaDecodeEntry(SDecoder *pCoder, SMetaEntry *pME) {
  TAOS_CHECK_RETURN(tStartDecode(pCoder));
  TAOS_CHECK_RETURN(tDecodeI64(pCoder, &pME->version));
  TAOS_CHECK_RETURN(tDecodeI8(pCoder, &pME->type));
  TAOS_CHECK_RETURN(tDecodeI64(pCoder, &pME->uid));

  if (pME->type > 0) {
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
    if (!tDecodeIsEnd(pCoder)) {
      TAOS_CHECK_RETURN(metaDecodeExtSchemas(pCoder, pME));
    }
  }


  tEndDecode(pCoder);
  return 0;
}

static int32_t metaCloneSchema(const SSchemaWrapper *pSrc, SSchemaWrapper *pDst) {
  if (pSrc == NULL || pDst == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  pDst->nCols = pSrc->nCols;
  pDst->version = pSrc->version;
  pDst->pSchema = (SSchema *)taosMemoryMalloc(pSrc->nCols * sizeof(SSchema));
  if (pDst->pSchema == NULL) {
    return terrno;
  }
  memcpy(pDst->pSchema, pSrc->pSchema, pSrc->nCols * sizeof(SSchema));
  return TSDB_CODE_SUCCESS;
}

static void metaCloneSchemaFree(SSchemaWrapper *pSchema) {
  if (pSchema) {
    taosMemoryFreeClear(pSchema->pSchema);
  }
}

void metaCloneEntryFree(SMetaEntry **ppEntry) {
  if (ppEntry == NULL || *ppEntry == NULL) {
    return;
  }

  taosMemoryFreeClear((*ppEntry)->name);

  if ((*ppEntry)->type < 0) {
    taosMemoryFreeClear(*ppEntry);
    return;
  }

  if (TSDB_SUPER_TABLE == (*ppEntry)->type) {
    metaCloneSchemaFree(&(*ppEntry)->stbEntry.schemaRow);
    metaCloneSchemaFree(&(*ppEntry)->stbEntry.schemaTag);
  } else if (TSDB_CHILD_TABLE == (*ppEntry)->type) {
    taosMemoryFreeClear((*ppEntry)->ctbEntry.comment);
    taosMemoryFreeClear((*ppEntry)->ctbEntry.pTags);
  } else if (TSDB_NORMAL_TABLE == (*ppEntry)->type) {
    metaCloneSchemaFree(&(*ppEntry)->ntbEntry.schemaRow);
    taosMemoryFreeClear((*ppEntry)->ntbEntry.comment);
  } else {
    return;
  }
  metaCloneColCmprFree(&(*ppEntry)->colCmpr);

  taosMemoryFreeClear(*ppEntry);
  return;
}

int32_t metaCloneEntry(const SMetaEntry *pEntry, SMetaEntry **ppEntry) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (NULL == pEntry || NULL == ppEntry) {
    return TSDB_CODE_INVALID_PARA;
  }

  *ppEntry = (SMetaEntry *)taosMemoryCalloc(1, sizeof(SMetaEntry));
  if (NULL == *ppEntry) {
    return terrno;
  }

  (*ppEntry)->version = pEntry->version;
  (*ppEntry)->type = pEntry->type;
  (*ppEntry)->uid = pEntry->uid;

  if (pEntry->type < 0) {
    return TSDB_CODE_SUCCESS;
  }

  if (pEntry->name) {
    (*ppEntry)->name = tstrdup(pEntry->name);
    if (NULL == (*ppEntry)->name) {
      code = terrno;
      metaCloneEntryFree(ppEntry);
      return code;
    }
  }

  if (pEntry->type == TSDB_SUPER_TABLE) {
    (*ppEntry)->flags = pEntry->flags;

    code = metaCloneSchema(&pEntry->stbEntry.schemaRow, &(*ppEntry)->stbEntry.schemaRow);
    if (code) {
      metaCloneEntryFree(ppEntry);
      return code;
    }

    code = metaCloneSchema(&pEntry->stbEntry.schemaTag, &(*ppEntry)->stbEntry.schemaTag);
    if (code) {
      metaCloneEntryFree(ppEntry);
      return code;
    }
  } else if (pEntry->type == TSDB_CHILD_TABLE) {
    (*ppEntry)->ctbEntry.btime = pEntry->ctbEntry.btime;
    (*ppEntry)->ctbEntry.ttlDays = pEntry->ctbEntry.ttlDays;
    (*ppEntry)->ctbEntry.suid = pEntry->ctbEntry.suid;

    // comment
    (*ppEntry)->ctbEntry.commentLen = pEntry->ctbEntry.commentLen;
    if (pEntry->ctbEntry.commentLen > 0) {
      (*ppEntry)->ctbEntry.comment = taosMemoryMalloc(pEntry->ctbEntry.commentLen + 1);
      if (NULL == (*ppEntry)->ctbEntry.comment) {
        code = terrno;
        metaCloneEntryFree(ppEntry);
        return code;
      }
      memcpy((*ppEntry)->ctbEntry.comment, pEntry->ctbEntry.comment, pEntry->ctbEntry.commentLen + 1);
    }

    // tags
    STag *pTags = (STag *)pEntry->ctbEntry.pTags;
    (*ppEntry)->ctbEntry.pTags = taosMemoryCalloc(1, pTags->len);
    if (NULL == (*ppEntry)->ctbEntry.pTags) {
      code = terrno;
      metaCloneEntryFree(ppEntry);
      return code;
    }
    memcpy((*ppEntry)->ctbEntry.pTags, pEntry->ctbEntry.pTags, pTags->len);
  } else if (pEntry->type == TSDB_NORMAL_TABLE) {
    (*ppEntry)->ntbEntry.btime = pEntry->ntbEntry.btime;
    (*ppEntry)->ntbEntry.ttlDays = pEntry->ntbEntry.ttlDays;
    (*ppEntry)->ntbEntry.ncid = pEntry->ntbEntry.ncid;

    // schema
    code = metaCloneSchema(&pEntry->ntbEntry.schemaRow, &(*ppEntry)->ntbEntry.schemaRow);
    if (code) {
      metaCloneEntryFree(ppEntry);
      return code;
    }

    // comment
    (*ppEntry)->ntbEntry.commentLen = pEntry->ntbEntry.commentLen;
    if (pEntry->ntbEntry.commentLen > 0) {
      (*ppEntry)->ntbEntry.comment = taosMemoryMalloc(pEntry->ntbEntry.commentLen + 1);
      if (NULL == (*ppEntry)->ntbEntry.comment) {
        code = terrno;
        metaCloneEntryFree(ppEntry);
        return code;
      }
      memcpy((*ppEntry)->ntbEntry.comment, pEntry->ntbEntry.comment, pEntry->ntbEntry.commentLen + 1);
    }
  } else {
    return TSDB_CODE_INVALID_PARA;
  }

  code = metaCloneColCmpr(&pEntry->colCmpr, &(*ppEntry)->colCmpr);
  if (code) {
    metaCloneEntryFree(ppEntry);
    return code;
  }

  return code;
}
