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
#include "osMemPool.h"
#include "osMemory.h"
#include "tencode.h"
#include "tmsg.h"

static bool schemasHasTypeMod(const SSchema *pSchema, int32_t nCols) {
  for (int32_t i = 0; i < nCols; i++) {
    if (HAS_TYPE_MOD(pSchema + i)) {
      return true;
    }
  }
  return false;
}

int meteEncodeColRefEntry(SEncoder *pCoder, const SMetaEntry *pME) {
  const SColRefWrapper *pw = &pME->colRef;
  TAOS_CHECK_RETURN(tEncodeI32v(pCoder, pw->nCols));
  TAOS_CHECK_RETURN(tEncodeI32v(pCoder, pw->version));
  uTrace("encode cols:%d", pw->nCols);

  for (int32_t i = 0; i < pw->nCols; i++) {
    SColRef *p = &pw->pColRef[i];
    TAOS_CHECK_RETURN(tEncodeI8(pCoder, p->hasRef));
    TAOS_CHECK_RETURN(tEncodeI16v(pCoder, p->id));
    if (p->hasRef) {
      TAOS_CHECK_RETURN(tEncodeCStr(pCoder, p->refDbName));
      TAOS_CHECK_RETURN(tEncodeCStr(pCoder, p->refTableName));
      TAOS_CHECK_RETURN(tEncodeCStr(pCoder, p->refColName));
    }
  }
  return 0;
}

SExtSchema* metaGetSExtSchema(const SMetaEntry *pME) {
  const SSchemaWrapper *pSchWrapper = NULL;
  bool                  hasTypeMods = false;
  if (pME->type == TSDB_SUPER_TABLE) {
    pSchWrapper = &pME->stbEntry.schemaRow;
  } else if (pME->type == TSDB_NORMAL_TABLE) {
    pSchWrapper = &pME->ntbEntry.schemaRow;
  } else {
    return NULL;
  }
  hasTypeMods = schemasHasTypeMod(pSchWrapper->pSchema, pSchWrapper->nCols);

  if (hasTypeMods) {
    SExtSchema *ret = taosMemoryMalloc(sizeof(SExtSchema) * pSchWrapper->nCols);
    if (ret != NULL) {
      memcpy(ret, pSchWrapper->pExtSchema, pSchWrapper->nCols * sizeof(SExtSchema));
    }
    return ret;
  }
  return NULL;
}

int meteDecodeColRefEntry(SDecoder *pDecoder, SMetaEntry *pME) {
  SColRefWrapper *pWrapper = &pME->colRef;
  TAOS_CHECK_RETURN(tDecodeI32v(pDecoder, &pWrapper->nCols));
  if (pWrapper->nCols == 0) {
    return 0;
  }

  TAOS_CHECK_RETURN(tDecodeI32v(pDecoder, &pWrapper->version));
  uDebug("decode cols:%d", pWrapper->nCols);
  pWrapper->pColRef = (SColRef *)tDecoderMalloc(pDecoder, pWrapper->nCols * sizeof(SColRef));
  if (pWrapper->pColRef == NULL) {
    return terrno;
  }

  for (int i = 0; i < pWrapper->nCols; i++) {
    SColRef *p = &pWrapper->pColRef[i];
    TAOS_CHECK_RETURN(tDecodeI8(pDecoder, (int8_t *)&p->hasRef));
    TAOS_CHECK_RETURN(tDecodeI16v(pDecoder, &p->id));
    if (p->hasRef) {
      TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, p->refDbName));
      TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, p->refTableName));
      TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, p->refColName));
    }
  }
  return 0;
}

static FORCE_INLINE int32_t metatInitDefaultSColRefWrapper(SDecoder *pDecoder, SColRefWrapper *pRef,
                                                            SSchemaWrapper *pSchema) {
  pRef->nCols = pSchema->nCols;
  if ((pRef->pColRef = (SColRef *)tDecoderMalloc(pDecoder, pRef->nCols * sizeof(SColRef))) == NULL) {
    return terrno;
  }

  for (int32_t i = 0; i < pRef->nCols; i++) {
    SColRef  *pColRef = &pRef->pColRef[i];
    SSchema  *pColSchema = &pSchema->pSchema[i];
    pColRef->id = pColSchema->colId;
    pColRef->hasRef = false;
  }
  return 0;
}

static int32_t metaCloneColRef(const SColRefWrapper*pSrc, SColRefWrapper *pDst) {
  if (pSrc->nCols > 0) {
    pDst->nCols = pSrc->nCols;
    pDst->version = pSrc->version;
    pDst->pColRef = (SColRef*)taosMemoryCalloc(pSrc->nCols, sizeof(SColRef));
    if (NULL == pDst->pColRef) {
      return terrno;
    }
    memcpy(pDst->pColRef, pSrc->pColRef, pSrc->nCols * sizeof(SColRef));
  }
  return 0;
}

static int32_t metaEncodeComprEntryImpl(SEncoder *pCoder, SColCmprWrapper *pw) {
  int32_t code = 0;
  TAOS_CHECK_RETURN(tEncodeI32v(pCoder, pw->nCols));
  TAOS_CHECK_RETURN(tEncodeI32v(pCoder, pw->version));
  uTrace("encode cols:%d", pw->nCols);

  for (int32_t i = 0; i < pw->nCols; i++) {
    SColCmpr *p = &pw->pColCmpr[i];
    TAOS_CHECK_RETURN(tEncodeI16v(pCoder, p->id));
    TAOS_CHECK_RETURN(tEncodeU32(pCoder, p->alg));
  }
  return code;
}
int meteEncodeColCmprEntry(SEncoder *pCoder, const SMetaEntry *pME) {
  const SColCmprWrapper *pw = &pME->colCmpr;
  return metaEncodeComprEntryImpl(pCoder, (SColCmprWrapper *)pw);
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

  if (pDecoder == NULL) {
    pCmpr->pColCmpr = taosMemoryCalloc(1, pCmpr->nCols * sizeof(SColCmpr));
  } else {
    pCmpr->pColCmpr = (SColCmpr *)tDecoderMalloc(pDecoder, pCmpr->nCols * sizeof(SColCmpr));
  }

  if (pCmpr->pColCmpr == NULL) {
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

static void metaCloneColRefFree(SColRefWrapper *pColRef) {
  if (pColRef) {
    taosMemoryFreeClear(pColRef->pColRef);
  }
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
    } else if (pME->type == TSDB_CHILD_TABLE || pME->type == TSDB_VIRTUAL_CHILD_TABLE) {
      TAOS_CHECK_RETURN(tEncodeI64(pCoder, pME->ctbEntry.btime));
      TAOS_CHECK_RETURN(tEncodeI32(pCoder, pME->ctbEntry.ttlDays));
      TAOS_CHECK_RETURN(tEncodeI32v(pCoder, pME->ctbEntry.commentLen));
      if (pME->ctbEntry.commentLen > 0) {
        TAOS_CHECK_RETURN(tEncodeCStr(pCoder, pME->ctbEntry.comment));
      }
      TAOS_CHECK_RETURN(tEncodeI64(pCoder, pME->ctbEntry.suid));
      TAOS_CHECK_RETURN(tEncodeTag(pCoder, (const STag *)pME->ctbEntry.pTags));
    } else if (pME->type == TSDB_NORMAL_TABLE || pME->type == TSDB_VIRTUAL_NORMAL_TABLE) {
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
    if (pME->type == TSDB_VIRTUAL_NORMAL_TABLE || pME->type == TSDB_VIRTUAL_CHILD_TABLE) {
      TAOS_CHECK_RETURN(meteEncodeColRefEntry(pCoder, pME));
    } else {
      if (pME->type == TSDB_SUPER_TABLE && TABLE_IS_COL_COMPRESSED(pME->flags)) {
        TAOS_CHECK_RETURN(meteEncodeColCmprEntry(pCoder, pME));
      } else if (pME->type == TSDB_NORMAL_TABLE) {
        if (pME->colCmpr.nCols != 0) {
          TAOS_CHECK_RETURN(meteEncodeColCmprEntry(pCoder, pME));
        } else {
          metaWarn("meta/entry: failed to get compress cols, type:%d", pME->type);
          SColCmprWrapper colCmprs = {0};
          int32_t code = metatInitDefaultSColCmprWrapper(NULL, &colCmprs, (SSchemaWrapper *)&pME->ntbEntry.schemaRow);
          if (code != 0) {
            taosMemoryFree(colCmprs.pColCmpr);
            TAOS_CHECK_RETURN(code);
          }
          code = metaEncodeComprEntryImpl(pCoder, &colCmprs);
          taosMemoryFree(colCmprs.pColCmpr);
          TAOS_CHECK_RETURN(code);
        }
      }
    }
  }
  if (pME->type == TSDB_SUPER_TABLE) {
    TAOS_CHECK_RETURN(tEncodeI64(pCoder, pME->stbEntry.keep));
  }

  tEndEncode(pCoder);
  return 0;
}

int metaDecodeEntryImpl(SDecoder *pCoder, SMetaEntry *pME, bool headerOnly) {
  TAOS_CHECK_RETURN(tStartDecode(pCoder));
  TAOS_CHECK_RETURN(tDecodeI64(pCoder, &pME->version));
  TAOS_CHECK_RETURN(tDecodeI8(pCoder, &pME->type));
  TAOS_CHECK_RETURN(tDecodeI64(pCoder, &pME->uid));

  if (headerOnly) {
    tEndDecode(pCoder);
    return 0;
  }

  if (pME->type > 0) {
    TAOS_CHECK_RETURN(tDecodeCStr(pCoder, &pME->name));

    if (pME->type == TSDB_SUPER_TABLE) {
      TAOS_CHECK_RETURN(tDecodeI8(pCoder, &pME->flags));
      TAOS_CHECK_RETURN(tDecodeSSchemaWrapperEx(pCoder, &pME->stbEntry.schemaRow));
      TAOS_CHECK_RETURN(tDecodeSSchemaWrapperEx(pCoder, &pME->stbEntry.schemaTag));
      if (TABLE_IS_ROLLUP(pME->flags)) {
        TAOS_CHECK_RETURN(tDecodeSRSmaParam(pCoder, &pME->stbEntry.rsmaParam));
      }
    } else if (pME->type == TSDB_CHILD_TABLE || pME->type == TSDB_VIRTUAL_CHILD_TABLE) {
      TAOS_CHECK_RETURN(tDecodeI64(pCoder, &pME->ctbEntry.btime));
      TAOS_CHECK_RETURN(tDecodeI32(pCoder, &pME->ctbEntry.ttlDays));
      TAOS_CHECK_RETURN(tDecodeI32v(pCoder, &pME->ctbEntry.commentLen));
      if (pME->ctbEntry.commentLen > 0) {
        TAOS_CHECK_RETURN(tDecodeCStr(pCoder, &pME->ctbEntry.comment));
      }
      TAOS_CHECK_RETURN(tDecodeI64(pCoder, &pME->ctbEntry.suid));
      TAOS_CHECK_RETURN(tDecodeTag(pCoder, (STag **)&pME->ctbEntry.pTags));
    } else if (pME->type == TSDB_NORMAL_TABLE || pME->type == TSDB_VIRTUAL_NORMAL_TABLE) {
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
    } else if (pME->type == TSDB_VIRTUAL_NORMAL_TABLE || pME->type == TSDB_VIRTUAL_CHILD_TABLE) {
      if (!tDecodeIsEnd(pCoder)) {
        uDebug("set type: %d, tableName:%s", pME->type, pME->name);
        TAOS_CHECK_RETURN(meteDecodeColRefEntry(pCoder, pME));
      } else {
        uDebug("set default type: %d, tableName:%s", pME->type, pME->name);
        if (pME->type == TSDB_VIRTUAL_NORMAL_TABLE) {
           TAOS_CHECK_RETURN(metatInitDefaultSColRefWrapper(pCoder, &pME->colRef, &pME->ntbEntry.schemaRow));
        }
      }
    }
  }
  if (pME->type == TSDB_SUPER_TABLE) {
    if (!tDecodeIsEnd(pCoder)) {
      TAOS_CHECK_RETURN(tDecodeI64(pCoder, &pME->stbEntry.keep));
    }
  }


  tEndDecode(pCoder);
  return 0;
}

int metaDecodeEntry(SDecoder *pCoder, SMetaEntry *pME) { return metaDecodeEntryImpl(pCoder, pME, false); }

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

  if (NULL != pSrc->pExtSchema) {
    pDst->pExtSchema = (SExtSchema *)taosMemoryMalloc(pSrc->nCols * sizeof(SExtSchema));
    if (pDst->pExtSchema == NULL) {
      taosMemoryFreeClear(pDst->pSchema);
      return terrno;
    }
    (void)memcpy(pDst->pExtSchema, pSrc->pExtSchema, pSrc->nCols * sizeof(SExtSchema));
  } else {
    pDst->pExtSchema = NULL;
  }

  return TSDB_CODE_SUCCESS;
}

static void metaCloneSchemaFree(SSchemaWrapper *pSchema) {
  if (pSchema) {
    taosMemoryFreeClear(pSchema->pSchema);
    taosMemoryFreeClear(pSchema->pExtSchema);
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
  } else if (TSDB_CHILD_TABLE == (*ppEntry)->type || TSDB_VIRTUAL_CHILD_TABLE == (*ppEntry)->type) {
    taosMemoryFreeClear((*ppEntry)->ctbEntry.comment);
    taosMemoryFreeClear((*ppEntry)->ctbEntry.pTags);
  } else if (TSDB_NORMAL_TABLE == (*ppEntry)->type || TSDB_VIRTUAL_NORMAL_TABLE == (*ppEntry)->type) {
    metaCloneSchemaFree(&(*ppEntry)->ntbEntry.schemaRow);
    taosMemoryFreeClear((*ppEntry)->ntbEntry.comment);
  } else {
    return;
  }
  metaCloneColCmprFree(&(*ppEntry)->colCmpr);
  metaCloneColRefFree(&(*ppEntry)->colRef);

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
    (*ppEntry)->stbEntry.keep = pEntry->stbEntry.keep;
  } else if (pEntry->type == TSDB_CHILD_TABLE || pEntry->type == TSDB_VIRTUAL_CHILD_TABLE) {
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
  } else if (pEntry->type == TSDB_NORMAL_TABLE || pEntry->type == TSDB_VIRTUAL_NORMAL_TABLE) {
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

  if (pEntry->type == TSDB_VIRTUAL_NORMAL_TABLE || pEntry->type == TSDB_VIRTUAL_CHILD_TABLE) {
    code = metaCloneColRef(&pEntry->colRef, &(*ppEntry)->colRef);
    if (code) {
      metaCloneEntryFree(ppEntry);
      return code;
    }
  } else {
    code = metaCloneColCmpr(&pEntry->colCmpr, &(*ppEntry)->colCmpr);
    if (code) {
      metaCloneEntryFree(ppEntry);
      return code;
    }
  }

  return code;
}
