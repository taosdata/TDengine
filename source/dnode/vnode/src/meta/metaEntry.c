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

extern SDmNotifyHandle dmNotifyHdl;

int metaEncodeEntry(SEncoder *encoder, const SMetaEntry *entry) {
  if (tStartEncode(encoder) < 0) return -1;

  if (tEncodeI64(encoder, entry->version) < 0) return -1;
  if (tEncodeI8(encoder, entry->type) < 0) return -1;
  if (tEncodeI64(encoder, entry->uid) < 0) return -1;
  if (entry->type > 0) {
    if (entry->name == NULL || tEncodeCStr(encoder, entry->name) < 0) return -1;

    if (entry->type == TSDB_SUPER_TABLE) {
      if (tEncodeI8(encoder, entry->flags) < 0) return -1;
      if (tEncodeSSchemaWrapper(encoder, &entry->stbEntry.schemaRow) < 0) return -1;
      if (tEncodeSSchemaWrapper(encoder, &entry->stbEntry.schemaTag) < 0) return -1;
      if (TABLE_IS_ROLLUP(entry->flags)) {
        if (tEncodeSRSmaParam(encoder, &entry->stbEntry.rsmaParam) < 0) return -1;
      }
    } else if (entry->type == TSDB_CHILD_TABLE) {
      if (tEncodeI64(encoder, entry->ctbEntry.btime) < 0) return -1;
      if (tEncodeI32(encoder, entry->ctbEntry.ttlDays) < 0) return -1;
      if (tEncodeI32v(encoder, entry->ctbEntry.commentLen) < 0) return -1;
      if (entry->ctbEntry.commentLen > 0) {
        if (tEncodeCStr(encoder, entry->ctbEntry.comment) < 0) return -1;
      }
      if (tEncodeI64(encoder, entry->ctbEntry.suid) < 0) return -1;
      if (tEncodeTag(encoder, (const STag *)entry->ctbEntry.pTags) < 0) return -1;
    } else if (entry->type == TSDB_NORMAL_TABLE) {
      if (tEncodeI64(encoder, entry->ntbEntry.btime) < 0) return -1;
      if (tEncodeI32(encoder, entry->ntbEntry.ttlDays) < 0) return -1;
      if (tEncodeI32v(encoder, entry->ntbEntry.commentLen) < 0) return -1;
      if (entry->ntbEntry.commentLen > 0) {
        if (tEncodeCStr(encoder, entry->ntbEntry.comment) < 0) return -1;
      }
      if (tEncodeI32v(encoder, entry->ntbEntry.ncid) < 0) return -1;
      if (tEncodeSSchemaWrapper(encoder, &entry->ntbEntry.schemaRow) < 0) return -1;
    } else if (entry->type == TSDB_TSMA_TABLE) {
      if (tEncodeTSma(encoder, entry->smaEntry.tsma) < 0) return -1;
    } else {
      metaError("meta/entry: invalide table type: %" PRId8 " encode failed.", entry->type);

      return -1;
    }
  }

  tEndEncode(encoder);
  return 0;
}

int metaDecodeEntry(SDecoder *decoder, SMetaEntry *entry) {
  if (tStartDecode(decoder) < 0) return -1;

  if (tDecodeI64(decoder, &entry->version) < 0) return -1;
  if (tDecodeI8(decoder, &entry->type) < 0) return -1;
  if (tDecodeI64(decoder, &entry->uid) < 0) return -1;
  if (entry->type > 0) {
    if (tDecodeCStr(decoder, &entry->name) < 0) return -1;

    if (entry->type == TSDB_SUPER_TABLE) {
      if (tDecodeI8(decoder, &entry->flags) < 0) return -1;
      if (tDecodeSSchemaWrapperEx(decoder, &entry->stbEntry.schemaRow) < 0) return -1;
      if (tDecodeSSchemaWrapperEx(decoder, &entry->stbEntry.schemaTag) < 0) return -1;
      if (TABLE_IS_ROLLUP(entry->flags)) {
        if (tDecodeSRSmaParam(decoder, &entry->stbEntry.rsmaParam) < 0) return -1;
      }
    } else if (entry->type == TSDB_CHILD_TABLE) {
      if (tDecodeI64(decoder, &entry->ctbEntry.btime) < 0) return -1;
      if (tDecodeI32(decoder, &entry->ctbEntry.ttlDays) < 0) return -1;
      if (tDecodeI32v(decoder, &entry->ctbEntry.commentLen) < 0) return -1;
      if (entry->ctbEntry.commentLen > 0) {
        if (tDecodeCStr(decoder, &entry->ctbEntry.comment) < 0) return -1;
      }
      if (tDecodeI64(decoder, &entry->ctbEntry.suid) < 0) return -1;
      if (tDecodeTag(decoder, (STag **)&entry->ctbEntry.pTags) < 0) return -1;
    } else if (entry->type == TSDB_NORMAL_TABLE) {
      if (tDecodeI64(decoder, &entry->ntbEntry.btime) < 0) return -1;
      if (tDecodeI32(decoder, &entry->ntbEntry.ttlDays) < 0) return -1;
      if (tDecodeI32v(decoder, &entry->ntbEntry.commentLen) < 0) return -1;
      if (entry->ntbEntry.commentLen > 0) {
        if (tDecodeCStr(decoder, &entry->ntbEntry.comment) < 0) return -1;
      }
      if (tDecodeI32v(decoder, &entry->ntbEntry.ncid) < 0) return -1;
      if (tDecodeSSchemaWrapperEx(decoder, &entry->ntbEntry.schemaRow) < 0) return -1;
    } else if (entry->type == TSDB_TSMA_TABLE) {
      entry->smaEntry.tsma = tDecoderMalloc(decoder, sizeof(STSma));
      if (!entry->smaEntry.tsma) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        return -1;
      }
      if (tDecodeTSma(decoder, entry->smaEntry.tsma, true) < 0) return -1;
    } else {
      metaError("meta/entry: invalide table type: %" PRId8 " decode failed.", entry->type);

      return -1;
    }
  }

  tEndDecode(decoder);
  return 0;
}

static inline void metaTimeSeriesNotifyCheck(SMeta *pMeta) {
#ifdef TD_ENTERPRISE
  int64_t nTimeSeries = metaGetTimeSeriesNum(pMeta, 0);
  int64_t deltaTS = nTimeSeries - pMeta->pVnode->config.vndStats.numOfReportedTimeSeries;
  if (deltaTS > tsTimeSeriesThreshold) {
    if (0 == atomic_val_compare_exchange_8(&dmNotifyHdl.state, 1, 2)) {
      tsem_post(&dmNotifyHdl.sem);
    }
  }
#endif
}

static int32_t tSchemaWrapperClone(const SSchemaWrapper *from, SSchemaWrapper *to) {
  to->nCols = from->nCols;
  to->version = from->version;
  to->pSchema = taosMemoryMalloc(to->nCols * sizeof(SSchema));
  if (to->pSchema == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  memcpy(to->pSchema, from->pSchema, to->nCols * sizeof(SSchema));
  return 0;
}

static int32_t tSchemaWrapperCloneDestroy(SSchemaWrapper *schema) {
  taosMemoryFree(schema->pSchema);
  return 0;
}

static int32_t tTagClone(const STag *from, STag **to) {
  *to = taosMemoryMalloc(from->len);
  if (*to == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  memcpy(*to, from, from->len);
  return 0;
}

static int32_t tTagCloneDestroy(STag *tag) {
  taosMemoryFree(tag);
  return 0;
}

int32_t metaEntryCloneDestroy(SMetaEntry *entry) {
  if (entry == NULL) return 0;
  taosMemoryFree(entry->name);
  if (entry->type == TSDB_SUPER_TABLE) {
    tSchemaWrapperCloneDestroy(&entry->stbEntry.schemaRow);
    tSchemaWrapperCloneDestroy(&entry->stbEntry.schemaTag);
  } else if (entry->type == TSDB_CHILD_TABLE) {
    taosMemoryFree(entry->ctbEntry.comment);
    tTagCloneDestroy((STag *)entry->ctbEntry.pTags);
  } else if (entry->type == TSDB_NORMAL_TABLE) {
    taosMemoryFree(entry->ntbEntry.comment);
    tSchemaWrapperCloneDestroy(&entry->ntbEntry.schemaRow);
  } else {
    ASSERT(0);
  }
  taosMemoryFree(entry);
  return 0;
}

int32_t metaEntryClone(const SMetaEntry *from, SMetaEntry **entry) {
  int32_t code = 0;
  int32_t lino = 0;

  if ((*entry = taosMemoryCalloc(1, sizeof(SMetaEntry))) == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
  }

  (*entry)->version = from->version;
  (*entry)->type = from->type;
  (*entry)->flags = from->flags;
  (*entry)->uid = from->uid;
  (*entry)->name = taosStrdup(from->name);
  if ((*entry)->name == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
  }

  if (from->type == TSDB_SUPER_TABLE) {
    code = tSchemaWrapperClone(&from->stbEntry.schemaRow, &(*entry)->stbEntry.schemaRow);
    TSDB_CHECK_CODE(code, lino, _exit);
    code = tSchemaWrapperClone(&from->stbEntry.schemaTag, &(*entry)->stbEntry.schemaTag);
    TSDB_CHECK_CODE(code, lino, _exit);
    (*entry)->stbEntry.rsmaParam = from->stbEntry.rsmaParam;
  } else if (from->type == TSDB_CHILD_TABLE) {
    (*entry)->ctbEntry.btime = from->ctbEntry.btime;
    (*entry)->ctbEntry.ttlDays = from->ctbEntry.ttlDays;
    (*entry)->ctbEntry.commentLen = from->ctbEntry.commentLen;
    if (from->ctbEntry.commentLen > 0) {
      (*entry)->ctbEntry.comment = taosMemoryCalloc(1, from->ctbEntry.commentLen + 1);
      if ((*entry)->ctbEntry.comment == NULL) {
        TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
      }
      memcpy((*entry)->ctbEntry.comment, from->ctbEntry.comment, from->ctbEntry.commentLen);
    }
    (*entry)->ctbEntry.suid = from->ctbEntry.suid;
    code = tTagClone((const STag *)from->ctbEntry.pTags, (STag **)&(*entry)->ctbEntry.pTags);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else if (from->type == TSDB_NORMAL_TABLE) {
    (*entry)->ntbEntry.btime = from->ntbEntry.btime;
    (*entry)->ntbEntry.ttlDays = from->ntbEntry.ttlDays;
    (*entry)->ntbEntry.commentLen = from->ntbEntry.commentLen;
    if (from->ntbEntry.commentLen > 0) {
      (*entry)->ntbEntry.comment = taosMemoryCalloc(1, from->ntbEntry.commentLen + 1);
      if ((*entry)->ntbEntry.comment == NULL) {
        TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
      }
      memcpy((*entry)->ntbEntry.comment, from->ntbEntry.comment, from->ntbEntry.commentLen);
    }
    (*entry)->ntbEntry.ncid = from->ntbEntry.ncid;
    code = tSchemaWrapperClone(&from->ntbEntry.schemaRow, &(*entry)->ntbEntry.schemaRow);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    ASSERT(0);
  }

_exit:
  if (code) {
    metaError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    metaEntryCloneDestroy(*entry);
    *entry = NULL;
  }
  return code;
}

int32_t metaGetTableEntryByUidImpl(SMeta *meta, int64_t uid, SMetaEntry **entry) {
  int32_t code = 0;
  int32_t lino = 0;
  void   *value = NULL;
  int32_t valueSize = 0;

  // get latest version
  if (tdbTbGet(meta->pUidIdx, &uid, sizeof(uid), &value, &valueSize) != 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_NOT_FOUND, lino, _exit);
  }

  // get encoded entry
  SUidIdxVal *uidIdxVal = (SUidIdxVal *)value;
  if (tdbTbGet(meta->pTbDb,
               &(STbDbKey){
                   .uid = uid,
                   .version = uidIdxVal->version,
               },
               sizeof(STbDbKey), &value, &valueSize) != 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_OP_ERROR, lino, _exit);
  }

  // decode entry
  SMetaEntry dEntry = {0};
  SDecoder   decoder = {0};

  tDecoderInit(&decoder, value, valueSize);

  if (metaDecodeEntry(&decoder, &dEntry) != 0) {
    tDecoderClear(&decoder);
    TSDB_CHECK_CODE(code = TSDB_CODE_MSG_DECODE_ERROR, lino, _exit);
  }

  // clone entry
  code = metaEntryClone(&dEntry, entry);
  if (code) {
    tDecoderClear(&decoder);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  tDecoderClear(&decoder);

_exit:
  tdbFree(value);
  return code;
}

int32_t metaGetTableEntryByNameImpl(SMeta *meta, const char *name, SMetaEntry **entry) {
  int32_t code = 0;
  int32_t lino = 0;
  void   *value = NULL;
  int32_t valueSize = 0;

  if (tdbTbGet(meta->pNameIdx, name, strlen(name) + 1, &value, &valueSize) != 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_NOT_FOUND, lino, _exit);
  }

  code = metaGetTableEntryByUidImpl(meta, *(int64_t *)value, entry);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  tdbFree(value);
  return code;
}

// =======================

static int32_t metaTagIdxKeyBuild(SMeta *meta, int64_t suid, int64_t uid, const STag *tags, const SSchema *column,
                                  STagIdxKey **key, int32_t *size) {
  int32_t code = 0;
  int32_t lino = 0;

  void   *tagData = NULL;
  int32_t tagDataSize = 0;

  STagVal tv = {
      .cid = column->colId,
  };

  if (tTagGet(tags, &tv)) {
    if (IS_VAR_DATA_TYPE(column->type)) {
      tagData = tv.pData;
      tagDataSize = tv.nData;
    } else {
      tagData = &tv.i64;
      tagDataSize = tDataTypes[column->type].bytes;
    }
  } else if (!IS_VAR_DATA_TYPE(column->type)) {
    tagDataSize = tDataTypes[column->type].bytes;
  }

  if (IS_VAR_DATA_TYPE(column->type)) {
    *size = sizeof(STagIdxKey) + tagDataSize + VARSTR_HEADER_SIZE + sizeof(tb_uid_t);
  } else {
    *size = sizeof(STagIdxKey) + tagDataSize + sizeof(tb_uid_t);
  }

  if ((*key = taosMemoryCalloc(1, *size)) == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
  }

  (*key)->suid = suid;
  (*key)->cid = column->colId;
  (*key)->isNull = (tagData == NULL) ? 1 : 0;
  (*key)->type = column->type;

  uint8_t *data = (*key)->data;
  if (IS_VAR_DATA_TYPE(column->type)) {
    *(uint16_t *)data = tagDataSize;
    data += VARSTR_HEADER_SIZE;
  }
  if (tagData) {
    memcpy(data, tagData, tagDataSize);
  }
  data += tagDataSize;
  *(int64_t *)data = uid;

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaTagIdxKeyDestroy(STagIdxKey *key) {
  taosMemoryFree(key);
  return 0;
}

static int32_t metaCreateTagColumnIndex(SMeta *meta, int64_t suid, int64_t uid, const STag *tags,
                                        const SSchema *column) {
  int32_t code = 0;
  int32_t lino = 0;

  STagIdxKey *tagIdxKey = NULL;
  int32_t     tagIdxKeySize = 0;

  code = metaTagIdxKeyBuild(meta, suid, uid, tags, column, &tagIdxKey, &tagIdxKeySize);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (tdbTbUpsert(meta->pTagIdx, tagIdxKey, tagIdxKeySize, NULL, 0, meta->txn) != 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_OP_ERROR, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  metaTagIdxKeyDestroy(tagIdxKey);
  return code;
}

static int32_t metaDropTagColumnIndex(SMeta *meta, int64_t suid, int64_t uid, const STag *tags, const SSchema *column) {
  int32_t code = 0;
  int32_t lino = 0;

  STagIdxKey *tagIdxKey = NULL;
  int32_t     tagIdxKeySize = 0;

  code = metaTagIdxKeyBuild(meta, suid, uid, tags, column, &tagIdxKey, &tagIdxKeySize);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (tdbTbDelete(meta->pTagIdx, tagIdxKey, tagIdxKeySize, meta->txn) != 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_OP_ERROR, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  metaTagIdxKeyDestroy(tagIdxKey);
  return code;
}

static int32_t metaUpsertNormalTagIndex(SMeta *meta, const SMetaEntry *childTableEntry,
                                        const SMetaEntry *superTableEntry) {
  int32_t code = 0;
  int32_t lino = 0;

  const SSchemaWrapper *tagSchema = &superTableEntry->stbEntry.schemaTag;
  const STag           *tags = (STag *)childTableEntry->ctbEntry.pTags;

  for (int i = 0; i < tagSchema->nCols; i++) {
    if (!IS_IDX_ON(&tagSchema->pSchema[i])) {
      continue;
    }

    code = metaCreateTagColumnIndex(meta, superTableEntry->uid, childTableEntry->uid, tags, &tagSchema->pSchema[i]);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaDropNormalTagIndex(SMeta *meta, const SMetaEntry *childTableEntry,
                                      const SMetaEntry *superTableEntry) {
  int32_t code = 0;
  int32_t lino = 0;

  const SSchemaWrapper *tagSchema = &superTableEntry->stbEntry.schemaTag;
  const STag           *tags = (STag *)childTableEntry->ctbEntry.pTags;
  STagIdxKey           *tagIdxKey = NULL;
  int32_t               tagIdxKeySize = 0;

  for (int i = 0; i < tagSchema->nCols; i++) {
    if (!IS_IDX_ON(&tagSchema->pSchema[i])) {
      continue;
    }

    metaTagIdxKeyDestroy(tagIdxKey);
    tagIdxKey = NULL;

    code = metaTagIdxKeyBuild(meta, superTableEntry->uid, childTableEntry->uid, tags, &tagSchema->pSchema[i],
                              &tagIdxKey, &tagIdxKeySize);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (tdbTbDelete(meta->pTagIdx, tagIdxKey, tagIdxKeySize, meta->txn) != 0) {
      TSDB_CHECK_CODE(code = TSDB_CODE_TDB_OP_ERROR, lino, _exit);
    }
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  metaTagIdxKeyDestroy(tagIdxKey);
  return code;
}

static int32_t metaUpsertJsonTagIndex(SMeta *meta, const SMetaEntry *childTableEntry,
                                      const SMetaEntry *superTableEntry) {
  int32_t code = 0;
  int32_t lino = 0;

#ifdef USE_INVERTED_INDEX
  if (meta->pTagIvtIdx == NULL || childTableEntry == NULL) {
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  void       *data = childTableEntry->ctbEntry.pTags;
  const char *tagName = superTableEntry->stbEntry.schemaTag.pSchema->name;

  tb_uid_t    suid = childTableEntry->ctbEntry.suid;
  tb_uid_t    tuid = childTableEntry->uid;
  const void *pTagData = childTableEntry->ctbEntry.pTags;
  int32_t     nTagData = 0;

  SArray *pTagVals = NULL;
  code = tTagToValArray((const STag *)data, &pTagVals);
  TSDB_CHECK_CODE(code, lino, _exit);

  SIndexMultiTerm *terms = indexMultiTermCreate();
  int16_t          nCols = taosArrayGetSize(pTagVals);
  for (int i = 0; i < nCols; i++) {
    STagVal *pTagVal = (STagVal *)taosArrayGet(pTagVals, i);
    char     type = pTagVal->type;

    char   *key = pTagVal->pKey;
    int32_t nKey = strlen(key);

    SIndexTerm *term = NULL;
    if (type == TSDB_DATA_TYPE_NULL) {
      term = indexTermCreate(suid, ADD_VALUE, TSDB_DATA_TYPE_VARCHAR, key, nKey, NULL, 0);
    } else if (type == TSDB_DATA_TYPE_NCHAR) {
      if (pTagVal->nData > 0) {
        char   *val = taosMemoryCalloc(1, pTagVal->nData + VARSTR_HEADER_SIZE);
        int32_t len = taosUcs4ToMbs((TdUcs4 *)pTagVal->pData, pTagVal->nData, val + VARSTR_HEADER_SIZE);
        memcpy(val, (uint16_t *)&len, VARSTR_HEADER_SIZE);
        type = TSDB_DATA_TYPE_VARCHAR;
        term = indexTermCreate(suid, ADD_VALUE, type, key, nKey, val, len);
        taosMemoryFree(val);
      } else if (pTagVal->nData == 0) {
        term = indexTermCreate(suid, ADD_VALUE, TSDB_DATA_TYPE_VARCHAR, key, nKey, (const char *)pTagVal->pData, 0);
      }
    } else if (type == TSDB_DATA_TYPE_DOUBLE) {
      double val = *(double *)(&pTagVal->i64);
      int    len = sizeof(val);
      term = indexTermCreate(suid, ADD_VALUE, type, key, nKey, (const char *)&val, len);
    } else if (type == TSDB_DATA_TYPE_BOOL) {
      int val = *(int *)(&pTagVal->i64);
      int len = sizeof(val);
      term = indexTermCreate(suid, ADD_VALUE, TSDB_DATA_TYPE_BOOL, key, nKey, (const char *)&val, len);
    }
    if (term != NULL) {
      indexMultiTermAdd(terms, term);
    }
  }
  indexJsonPut(meta->pTagIvtIdx, terms, tuid);
  indexMultiTermDestroy(terms);
  taosArrayDestroy(pTagVals);
#endif

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaDropJsonTagIndex(SMeta *meta, const SMetaEntry *childTableEntry, const SMetaEntry *superTableEntry) {
#ifdef USE_INVERTED_INDEX
  if (meta->pTagIvtIdx == NULL || childTableEntry == NULL) {
    return -1;
  }
  const SSchemaWrapper *tagSchema = &superTableEntry->stbEntry.schemaTag;
  void                 *data = childTableEntry->ctbEntry.pTags;
  const char           *tagName = tagSchema->pSchema[0].name;

  tb_uid_t    suid = childTableEntry->ctbEntry.suid;
  tb_uid_t    tuid = childTableEntry->uid;
  const void *pTagData = childTableEntry->ctbEntry.pTags;
  int32_t     nTagData = 0;

  SArray *pTagVals = NULL;
  if (tTagToValArray((const STag *)data, &pTagVals) != 0) {
    return -1;
  }

  SIndexMultiTerm *terms = indexMultiTermCreate();
  int16_t          nCols = taosArrayGetSize(pTagVals);
  for (int i = 0; i < nCols; i++) {
    STagVal *pTagVal = (STagVal *)taosArrayGet(pTagVals, i);
    char     type = pTagVal->type;

    char   *key = pTagVal->pKey;
    int32_t nKey = strlen(key);

    SIndexTerm *term = NULL;
    if (type == TSDB_DATA_TYPE_NULL) {
      term = indexTermCreate(suid, DEL_VALUE, TSDB_DATA_TYPE_VARCHAR, key, nKey, NULL, 0);
    } else if (type == TSDB_DATA_TYPE_NCHAR) {
      if (pTagVal->nData > 0) {
        char   *val = taosMemoryCalloc(1, pTagVal->nData + VARSTR_HEADER_SIZE);
        int32_t len = taosUcs4ToMbs((TdUcs4 *)pTagVal->pData, pTagVal->nData, val + VARSTR_HEADER_SIZE);
        memcpy(val, (uint16_t *)&len, VARSTR_HEADER_SIZE);
        type = TSDB_DATA_TYPE_VARCHAR;
        term = indexTermCreate(suid, DEL_VALUE, type, key, nKey, val, len);
        taosMemoryFree(val);
      } else if (pTagVal->nData == 0) {
        term = indexTermCreate(suid, DEL_VALUE, TSDB_DATA_TYPE_VARCHAR, key, nKey, pTagVal->pData, 0);
      }
    } else if (type == TSDB_DATA_TYPE_DOUBLE) {
      double val = *(double *)(&pTagVal->i64);
      int    len = sizeof(val);
      term = indexTermCreate(suid, DEL_VALUE, type, key, nKey, (const char *)&val, len);
    } else if (type == TSDB_DATA_TYPE_BOOL) {
      int val = *(int *)(&pTagVal->i64);
      int len = sizeof(val);
      term = indexTermCreate(suid, DEL_VALUE, TSDB_DATA_TYPE_BOOL, key, nKey, (const char *)&val, len);
    }
    if (term != NULL) {
      indexMultiTermAdd(terms, term);
    }
  }
  indexJsonPut(meta->pTagIvtIdx, terms, tuid);
  indexMultiTermDestroy(terms);
  taosArrayDestroy(pTagVals);
#endif
  return 0;
}

/* table.db */
static int32_t metaUpsertTableEntry(SMeta *meta, const SMetaEntry *entry) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SEncoder encoder = {0};
  void    *value = NULL;
  int32_t  valueSize = 0;

  // encode
  tEncodeSize(metaEncodeEntry, entry, valueSize, code);
  if (code < 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_MSG_ENCODE_ERROR, lino, _exit);
  }

  if ((value = taosMemoryMalloc(valueSize)) == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
  }

  tEncoderInit(&encoder, value, valueSize);
  if (metaEncodeEntry(&encoder, entry) != 0) {
    tEncoderClear(&encoder);
    TSDB_CHECK_CODE(code = TSDB_CODE_MSG_ENCODE_ERROR, lino, _exit);
  }
  tEncoderClear(&encoder);

  // insert
  if (tdbTbInsert(meta->pTbDb,
                  &(STbDbKey){
                      .version = entry->version,
                      .uid = entry->uid,
                  },
                  sizeof(STbDbKey), value, valueSize, meta->txn) != 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_OP_ERROR, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  taosMemoryFree(value);
  return code;
}

static int32_t metaDropTableEntry(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

  if (tdbTbDelete(meta->pTbDb,
                  &(STbDbKey){
                      .uid = entry->uid,
                      .version = entry->version,
                  },
                  sizeof(STbDbKey), meta->txn) != 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_OP_ERROR, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

/* uid.idx */
static void metaBuildEntryInfo(const SMetaEntry *entry, SMetaInfo *info) {
  info->uid = entry->uid;
  info->version = entry->version;
  if (entry->type == TSDB_SUPER_TABLE) {
    info->suid = entry->uid;
    info->skmVer = entry->stbEntry.schemaRow.version;
  } else if (entry->type == TSDB_CHILD_TABLE) {
    info->suid = entry->ctbEntry.suid;
    info->skmVer = 0;
  } else if (entry->type == TSDB_NORMAL_TABLE) {
    info->suid = 0;
    info->skmVer = entry->ntbEntry.schemaRow.version;
  } else {
    ASSERT(0);
  }
}

static int32_t metaUpsertUidIdx(SMeta *meta, const SMetaEntry *entry) {
  int32_t   code = 0;
  int32_t   lino = 0;
  SMetaInfo info;

  metaBuildEntryInfo(entry, &info);

  // upsert cache
  code = metaCacheUpsert(meta, &info);
  TSDB_CHECK_CODE(code, lino, _exit);

  // put to tdb
  if (tdbTbUpsert(meta->pUidIdx, &entry->uid, sizeof(entry->uid),
                  &(SUidIdxVal){
                      .suid = info.suid,
                      .version = info.version,
                      .skmVer = info.skmVer,
                  },
                  sizeof(SUidIdxVal), meta->txn) != 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_OP_ERROR, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaDropUidIdx(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

  metaCacheDrop(meta, entry->uid);

  if (tdbTbDelete(meta->pUidIdx, &entry->uid, sizeof(entry->uid), meta->txn) != 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_OP_ERROR, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaSearchUidIdx(SMeta *meta, int64_t uid, SUidIdxVal *uidIdxVal) {
  int32_t code = 0;
  int32_t lino = 0;
  void   *value = NULL;
  int32_t valueSize = 0;

  if (tdbTbGet(meta->pUidIdx, &uid, sizeof(uid), &value, &valueSize) != 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_NOT_FOUND, lino, _exit);
  }

  ASSERT(valueSize == sizeof(SUidIdxVal));

  if (uidIdxVal) {
    *uidIdxVal = *(SUidIdxVal *)value;
  }

_exit:
  tdbFree(value);
  return code;
}

/* name.idx */
static int32_t metaUpsertNameIdx(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

  if (tdbTbUpsert(meta->pNameIdx, entry->name, strlen(entry->name) + 1, &entry->uid, sizeof(entry->uid), meta->txn) !=
      0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_OP_ERROR, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaDropNameIdx(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

  if (tdbTbDelete(meta->pNameIdx, entry->name, strlen(entry->name) + 1, meta->txn) != 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_OP_ERROR, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

/* schema.db */
static int32_t metaUpsertSchema(SMeta *meta, const SMetaEntry *entry) {
  int32_t  code = 0;
  int32_t  lino = 0;
  void    *value = NULL;
  int32_t  valueSize = 0;
  SEncoder encoder = {0};

  const SSchemaWrapper *schema = NULL;
  if (entry->type == TSDB_SUPER_TABLE) {
    schema = &entry->stbEntry.schemaRow;
  } else if (entry->type == TSDB_NORMAL_TABLE) {
    schema = &entry->ntbEntry.schemaRow;
  } else {
    ASSERT(0);
  }

  // encode
  tEncodeSize(tEncodeSSchemaWrapper, schema, valueSize, code);
  if (code < 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_MSG_ENCODE_ERROR, lino, _exit);
  }
  if ((value = taosMemoryMalloc(valueSize)) == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
  }
  tEncoderInit(&encoder, value, valueSize);
  if (tEncodeSSchemaWrapper(&encoder, schema) != 0) {
    tEncoderClear(&encoder);
    TSDB_CHECK_CODE(code = TSDB_CODE_MSG_ENCODE_ERROR, lino, _exit);
  }
  tEncoderClear(&encoder);

  // put
  if (tdbTbUpsert(meta->pSkmDb,
                  &(SSkmDbKey){
                      .uid = entry->uid,
                      .sver = schema->version,
                  },
                  sizeof(SSkmDbKey), value, valueSize, meta->txn) != 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_OP_ERROR, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  taosMemoryFree(value);
  return code;
}

static int32_t metaDropSchema(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

  const SSchemaWrapper *schema = NULL;
  if (entry->type == TSDB_SUPER_TABLE) {
    schema = &entry->stbEntry.schemaRow;
  } else if (entry->type == TSDB_NORMAL_TABLE) {
    schema = &entry->ntbEntry.schemaRow;
  } else {
    ASSERT(0);
  }

  if (tdbTbDelete(meta->pSkmDb,
                  &(SSkmDbKey){
                      .uid = entry->uid,
                      .sver = schema->version,
                  },
                  sizeof(SSkmDbKey), meta->txn)) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_OP_ERROR, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

/* ctb.idx */
static int32_t metaUpsertCtbIdx(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

  ASSERT(entry->type == TSDB_CHILD_TABLE);

  STag *tags = (STag *)entry->ctbEntry.pTags;
  if (tdbTbUpsert(meta->pCtbIdx,
                  &(SCtbIdxKey){
                      .suid = entry->ctbEntry.suid,
                      .uid = entry->uid,
                  },
                  sizeof(SCtbIdxKey), tags, tags->len, meta->txn) != 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_OP_ERROR, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaDropCtbIdx(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

  ASSERT(entry->type == TSDB_CHILD_TABLE);

  if (tdbTbDelete(meta->pCtbIdx,
                  &(SCtbIdxKey){
                      .suid = entry->ctbEntry.suid,
                      .uid = entry->uid,
                  },
                  sizeof(SCtbIdxKey), meta->txn)) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_OP_ERROR, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

/* suid.idx */
static int32_t metaUpsertSuidIdx(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

  if (tdbTbUpsert(meta->pSuidIdx, &entry->uid, sizeof(entry->uid), NULL, 0, meta->txn) != 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_OP_ERROR, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaDropSuidIdx(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

  if (tdbTbDelete(meta->pSuidIdx, &entry->uid, sizeof(entry->uid), meta->txn) != 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_OP_ERROR, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

/* tag.idx */
static int32_t metaUpsertTagIdx(SMeta *meta, const SMetaEntry *childTableEntry, const SMetaEntry *superTableEntry) {
  int32_t code = 0;
  int32_t lino = 0;

  const SSchemaWrapper *tagSchema = &superTableEntry->stbEntry.schemaTag;
  if (tagSchema->nCols == 1 && tagSchema->pSchema[0].type == TSDB_DATA_TYPE_JSON) {
    code = metaUpsertJsonTagIndex(meta, childTableEntry, superTableEntry);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    code = metaUpsertNormalTagIndex(meta, childTableEntry, superTableEntry);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaDropTagIdx(SMeta *meta, const SMetaEntry *childTableEntry, const SMetaEntry *superTableEntry) {
  int32_t code = 0;
  int32_t lino = 0;

  const SSchemaWrapper *tagSchema = &superTableEntry->stbEntry.schemaTag;
  if (tagSchema->nCols == 1 && tagSchema->pSchema[0].type == TSDB_DATA_TYPE_JSON) {
    code = metaDropJsonTagIndex(meta, childTableEntry, superTableEntry);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    code = metaDropNormalTagIndex(meta, childTableEntry, superTableEntry);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

/* btime.idx */
static int32_t metaBuildBtimeIdxKey(const SMetaEntry *entry, SBtimeIdxKey *key) {
  int64_t btime;
  if (entry->type == TSDB_CHILD_TABLE) {
    btime = entry->ctbEntry.btime;
  } else if (entry->type == TSDB_NORMAL_TABLE) {
    btime = entry->ntbEntry.btime;
  } else {
    ASSERT(0);
  }

  key->btime = btime;
  key->uid = entry->uid;
  return 0;
}

static int32_t metaUpsertBtimeIdx(SMeta *meta, const SMetaEntry *entry) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SBtimeIdxKey key;

  metaBuildBtimeIdxKey(entry, &key);

  if (tdbTbUpsert(meta->pBtimeIdx, &key, sizeof(key), NULL, 0, meta->txn) != 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_OP_ERROR, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaDropBtimeIdx(SMeta *meta, const SMetaEntry *entry) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SBtimeIdxKey key;

  metaBuildBtimeIdxKey(entry, &key);
  if (tdbTbDelete(meta->pBtimeIdx, &key, sizeof(key), meta->txn) != 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_OP_ERROR, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

/* ttl.idx */
static int32_t metaUpsertTtlIdx(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

  STtlUpdTtlCtx ctx = {
      .uid = entry->uid,
      .pTxn = meta->txn,
  };
  if (entry->type == TSDB_CHILD_TABLE) {
    ctx.ttlDays = entry->ctbEntry.ttlDays;
    ctx.changeTimeMs = entry->ctbEntry.btime;
  } else if (entry->type == TSDB_NORMAL_TABLE) {
    ctx.ttlDays = entry->ntbEntry.ttlDays;
    ctx.changeTimeMs = entry->ntbEntry.btime;
  } else {
    ASSERT(0);
  }

  code = ttlMgrInsertTtl(meta->pTtlMgr, &ctx);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaDropTtlIdx(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

  STtlDelTtlCtx ctx = {
      .uid = entry->uid,
      .pTxn = meta->txn,
  };
  if (entry->type == TSDB_CHILD_TABLE) {
    ctx.ttlDays = entry->ctbEntry.ttlDays;
  } else if (entry->type == TSDB_NORMAL_TABLE) {
    ctx.ttlDays = entry->ntbEntry.ttlDays;
  } else {
    ASSERT(0);
  }

  code = ttlMgrDeleteTtl(meta->pTtlMgr, &ctx);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaHandleChildTableEntryDelete(SMeta *meta, const SMetaEntry *childTableEntry,
                                               const SMetaEntry *inputSuperTableEntry) {
  int32_t     code = 0;
  int32_t     lino = 0;
  SMetaEntry *superTableEntry = (SMetaEntry *)inputSuperTableEntry;

  if (superTableEntry == NULL) {
    code = metaGetTableEntryByUidImpl(meta, childTableEntry->ctbEntry.suid, &superTableEntry);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  /* ttl.idx */
  code = metaDropTtlIdx(meta, childTableEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  /* btime.idx */
  code = metaDropBtimeIdx(meta, childTableEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  /* tag.idx */
  code = metaDropTagIdx(meta, childTableEntry, superTableEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  /* ctb.idx */
  code = metaDropCtbIdx(meta, childTableEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  /* name.idx */
  code = metaDropNameIdx(meta, childTableEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  /* uid.idx */
  code = metaDropUidIdx(meta, childTableEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  meta->pVnode->config.vndStats.numOfCTables--;
#if 0
  metaUpdateStbStats(pMeta, e.ctbEntry.suid, -1, 0);
  metaUidCacheClear(pMeta, e.ctbEntry.suid);
  metaTbGroupCacheClear(pMeta, e.ctbEntry.suid);
#endif

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  if (inputSuperTableEntry == NULL) {
    metaEntryCloneDestroy(superTableEntry);
  }
  return code;
}

static int32_t metaGetChildTableUidList(SMeta *meta, int64_t suid, SArray **childTableUids) {
  int32_t code = 0;
  int32_t lino = 0;
  void   *key = NULL;
  int32_t keySize = 0;
  int32_t rc;

  if ((*childTableUids = taosArrayInit(0, sizeof(int64_t))) == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
  }

  TBC *cursor = NULL;
  if (tdbTbcOpen(meta->pCtbIdx, &cursor, NULL)) {
    TSDB_CHECK_CODE(code = TSDB_CODE_TDB_OP_ERROR, lino, _exit);
  }

  if (tdbTbcMoveTo(cursor,
                   &(SCtbIdxKey){
                       .suid = suid,
                       .uid = INT64_MIN,
                   },
                   sizeof(SCtbIdxKey), &rc) >= 0) {
    for (;;) {
      if (tdbTbcNext(cursor, &key, &keySize, NULL, NULL) < 0) {
        break;
      }

      SCtbIdxKey *ctbIdxKey = (SCtbIdxKey *)key;
      if (ctbIdxKey->suid < suid) {
        continue;
      } else if (ctbIdxKey->suid > suid) {
        break;
      } else {
        taosArrayPush(*childTableUids, &ctbIdxKey->uid);
      }
    }
  }
  tdbTbcClose(cursor);

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  tdbFree(key);
  return code;
}

static int32_t metaHandleSuperTableEntryDelete(SMeta *meta, const SMetaEntry *superTableEntry) {
  int32_t     code = 0;
  int32_t     lino = 0;
  SArray     *childTableUids = NULL;
  SMetaEntry *childTableEntry = NULL;

  code = metaGetChildTableUidList(meta, superTableEntry->uid, &childTableUids);
  TSDB_CHECK_CODE(code, lino, _exit);

  // drop child tables
  for (int i = 0; i < taosArrayGetSize(childTableUids); i++) {
    metaEntryCloneDestroy(childTableEntry);
    childTableEntry = NULL;

    int64_t uid = *(int64_t *)taosArrayGet(childTableUids, i);
    code = metaGetTableEntryByUidImpl(meta, uid, &childTableEntry);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = metaHandleChildTableEntryDelete(meta, childTableEntry, superTableEntry);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  /* suid.idx */
  code = metaDropSuidIdx(meta, superTableEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  /* name.idx */
  code = metaDropNameIdx(meta, superTableEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  /* uid.idx */
  code = metaDropUidIdx(meta, superTableEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  taosArrayDestroy(childTableUids);
  metaEntryCloneDestroy(childTableEntry);
  return code;
}

static int32_t metaHandleNormalTableEntryDelete(SMeta *meta, const SMetaEntry *normalTableEntry) {
  int32_t code = 0;
  int32_t lino = 0;

  /* ttl.idx */
  code = metaDropTtlIdx(meta, normalTableEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  /* btime.idx */
  code = metaDropBtimeIdx(meta, normalTableEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  /* name.idx */
  code = metaDropNameIdx(meta, normalTableEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  /* uid.idx */
  code = metaDropUidIdx(meta, normalTableEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  meta->pVnode->config.vndStats.numOfNTables--;
  meta->pVnode->config.vndStats.numOfNTimeSeries -= (normalTableEntry->ntbEntry.schemaRow.nCols - 1);

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metadHanleEntryDelete(SMeta *meta, const SMetaEntry *inputEntry) {
  int32_t code = 0;
  int32_t lino = 0;

  SMetaEntry *savedEntry = NULL;
  code = metaGetTableEntryByUidImpl(meta, inputEntry->uid, &savedEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  ASSERT(-inputEntry->type == savedEntry->type);

  if (savedEntry->type == TSDB_SUPER_TABLE) {
    code = metaHandleSuperTableEntryDelete(meta, savedEntry);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else if (savedEntry->type == TSDB_CHILD_TABLE) {
    code = metaHandleChildTableEntryDelete(meta, savedEntry, NULL);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else if (savedEntry->type == TSDB_NORMAL_TABLE) {
    code = metaHandleNormalTableEntryDelete(meta, savedEntry);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    ASSERT(0);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  metaEntryCloneDestroy(savedEntry);
  return code;
}

static int32_t metaCreateIndexOnTag(SMeta *meta, const SMetaEntry *superTableEntry, int32_t columnIndex) {
  int32_t     code = 0;
  int32_t     lino = 0;
  SMetaEntry *childTableEntry = NULL;
  SArray     *childTableUids = NULL;

  code = metaGetChildTableUidList(meta, superTableEntry->uid, &childTableUids);
  TSDB_CHECK_CODE(code, lino, _exit);

  for (int32_t i = 0; i < taosArrayGetSize(childTableUids); i++) {
    metaEntryCloneDestroy(childTableEntry);
    childTableEntry = NULL;

    int64_t uid = *(int64_t *)taosArrayGet(childTableUids, i);
    code = metaGetTableEntryByUidImpl(meta, uid, &childTableEntry);
    TSDB_CHECK_CODE(code, lino, _exit);

    ASSERT(childTableEntry->type == TSDB_CHILD_TABLE);

    code = metaCreateTagColumnIndex(meta, superTableEntry->uid, childTableEntry->uid, childTableEntry->ctbEntry.pTags,
                                    &superTableEntry->stbEntry.schemaTag.pSchema[columnIndex]);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  taosArrayDestroy(childTableUids);
  metaEntryCloneDestroy(childTableEntry);
  return code;
}

static int32_t metaDropIndexOnTag(SMeta *meta, const SMetaEntry *superTableEntry, int32_t columnIndex) {
  int32_t code = 0;
  int32_t lino = 0;

  SMetaEntry *childTableEntry = NULL;
  SArray     *childTableUids = NULL;
  code = metaGetChildTableUidList(meta, superTableEntry->uid, &childTableUids);
  TSDB_CHECK_CODE(code, lino, _exit);

  for (int32_t i = 0; i < taosArrayGetSize(childTableUids); i++) {
    metaEntryCloneDestroy(childTableEntry);
    childTableEntry = NULL;

    int64_t uid = *(int64_t *)taosArrayGet(childTableUids, i);

    code = metaGetTableEntryByUidImpl(meta, uid, &childTableEntry);
    TSDB_CHECK_CODE(code, lino, _exit);

    ASSERT(childTableEntry->type == TSDB_CHILD_TABLE);

    code = metaDropTagColumnIndex(meta, superTableEntry->uid, childTableEntry->uid, childTableEntry->ctbEntry.pTags,
                                  &superTableEntry->stbEntry.schemaTag.pSchema[columnIndex]);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  metaEntryCloneDestroy(childTableEntry);
  taosArrayDestroy(childTableUids);
  return code;
}

static int32_t metaAlterSuperTableSchema(SMeta *meta, const SMetaEntry *newEntry, const SMetaEntry *oldEntry) {
  int32_t code = 0;
  int32_t lino = 0;

  const SSchemaWrapper *newSchema = &newEntry->stbEntry.schemaRow;
  const SSchemaWrapper *oldSchema = &oldEntry->stbEntry.schemaRow;

  ASSERT(newSchema->version == oldSchema->version + 1);

  /* schema.db */
  code = metaUpsertSchema(meta, newEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (newSchema->nCols != oldSchema->nCols) {
    int32_t iNew = 0, iOld = 0;
    while (iNew < newSchema->nCols && iOld < oldSchema->nCols) {
      if (newSchema->pSchema[iNew].colId == oldSchema->pSchema[iOld].colId) {
        iNew++;
        iOld++;
      } else if (newSchema->pSchema[iNew].colId < oldSchema->pSchema[iOld].colId) {  // TODO: add a column
        iNew++;
      } else {  // delete a column
        iOld++;
      }
    }

    while (iNew < newSchema->nCols) {  // TODO: add a column
      iNew++;
    }

    while (iOld < oldSchema->nCols) {  // TODO: delete a column
      iOld++;
    }

    // time-series statistics
    if (!metaTbInFilterCache(meta, newEntry->name, 1)) {
#if 0
      int64_t numOfChildTables;
      metaUpdateStbStats(meta, newEntry->uid, 0, newSchema->nCols - oldSchema->nCols);
      metaGetStbStats(meta->pVnode, newEntry->uid, &numOfChildTables, NULL);
      meta->pVnode->config.vndStats.numOfTimeSeries += (numOfChildTables * (newSchema->nCols - oldSchema->nCols));
      metaTimeSeriesNotifyCheck(meta);
#endif
    }
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaAlterSuperTableTagSchema(SMeta *meta, const SMetaEntry *newSuperTableEntry,
                                            const SMetaEntry *oldSuperTableEntry) {
  int32_t code = 0;
  int32_t lino = 0;

  const SSchemaWrapper *newTagSchema = &newSuperTableEntry->stbEntry.schemaTag;
  const SSchemaWrapper *oldTagSchema = &oldSuperTableEntry->stbEntry.schemaTag;

  ASSERT(newTagSchema->version == oldTagSchema->version + 1);

  int32_t iNew = 0, iOld = 0;
  while (iNew < newTagSchema->nCols && iOld < oldTagSchema->nCols) {
    if (newTagSchema->pSchema[iNew].colId == oldTagSchema->pSchema[iOld].colId) {
      if (IS_IDX_ON(&newTagSchema->pSchema[iNew]) && !IS_IDX_ON(&oldTagSchema->pSchema[iOld])) {
        code = metaCreateIndexOnTag(meta, newSuperTableEntry, iNew);
        TSDB_CHECK_CODE(code, lino, _exit);
      } else if (!IS_IDX_ON(&newTagSchema->pSchema[iNew]) && IS_IDX_ON(&oldTagSchema->pSchema[iOld])) {
        code = metaDropIndexOnTag(meta, oldSuperTableEntry, iOld);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
      iNew++;
      iOld++;
    } else if (newTagSchema->pSchema[iNew].colId < oldTagSchema->pSchema[iOld].colId) {
      if (IS_IDX_ON(&newTagSchema->pSchema[iNew])) {
        code = metaCreateIndexOnTag(meta, newSuperTableEntry, iNew);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
      iNew++;
    } else {
      if (IS_IDX_ON(&oldTagSchema->pSchema[iOld])) {
        code = metaDropIndexOnTag(meta, oldSuperTableEntry, iOld);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
      iOld++;
    }
  }

  while (iNew < newTagSchema->nCols) {
    if (IS_IDX_ON(&newTagSchema->pSchema[iNew])) {
      code = metaCreateIndexOnTag(meta, newSuperTableEntry, iNew);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    iNew++;
  }

  while (iOld < oldTagSchema->nCols) {
    if (IS_IDX_ON(&oldTagSchema->pSchema[iOld])) {
      code = metaDropIndexOnTag(meta, oldSuperTableEntry, iOld);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    iOld++;
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaHandleSuperTableEntryUpdate(SMeta *meta, const SMetaEntry *newEntry, const SMetaEntry *oldEntry) {
  int32_t code = 0;
  int32_t lino = 0;

  // change schema
  if (newEntry->stbEntry.schemaRow.version != oldEntry->stbEntry.schemaRow.version) {
    code = metaAlterSuperTableSchema(meta, newEntry, oldEntry);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // change tag schema
  if (newEntry->stbEntry.schemaTag.version != oldEntry->stbEntry.schemaTag.version) {
    code = metaAlterSuperTableTagSchema(meta, newEntry, oldEntry);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

extern int tagIdxKeyCmpr(const void *pKey1, int kLen1, const void *pKey2, int kLen2);

static int32_t metaHandleChildTableNormalTagUpdate(SMeta *meta, const SMetaEntry *oldChildTableEntry,
                                                   const SMetaEntry *newChildTableEntry,
                                                   const SMetaEntry *superTableEntry) {
  int32_t code = 0;
  int32_t lino = 0;

  const SSchemaWrapper *tagSchema = &superTableEntry->stbEntry.schemaTag;
  STagIdxKey           *newTagIdxKey = NULL;
  int32_t               newTagIdxKeySize = 0;
  STagIdxKey           *oldTagIdxKey = NULL;
  int32_t               oldTagIdxKeySize = 0;

  const STag *oldTag = oldChildTableEntry->ctbEntry.pTags;
  const STag *newTag = newChildTableEntry->ctbEntry.pTags;
  bool        isTagChanged = false;

  for (int32_t i = 0; i < tagSchema->nCols; i++) {
    if (!IS_IDX_ON(&tagSchema->pSchema[i])) {
      continue;
    }

    metaTagIdxKeyDestroy(newTagIdxKey);
    metaTagIdxKeyDestroy(oldTagIdxKey);
    newTagIdxKey = NULL;
    oldTagIdxKey = NULL;

    code = metaTagIdxKeyBuild(meta, superTableEntry->uid, newChildTableEntry->uid, newTag, &tagSchema->pSchema[i],
                              &newTagIdxKey, &newTagIdxKeySize);
    TSDB_CHECK_CODE(code, lino, _exit);
    code = metaTagIdxKeyBuild(meta, superTableEntry->uid, oldChildTableEntry->uid, oldTag, &tagSchema->pSchema[i],
                              &oldTagIdxKey, &oldTagIdxKeySize);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (tagIdxKeyCmpr(newTagIdxKey, newTagIdxKeySize, oldTagIdxKey, oldTagIdxKeySize) != 0) {
      isTagChanged = true;
      if (tdbTbDelete(meta->pTagIdx, oldTagIdxKey, oldTagIdxKeySize, meta->txn) != 0) {
        TSDB_CHECK_CODE(code = TSDB_CODE_TDB_OP_ERROR, lino, _exit);
      }
      if (tdbTbUpsert(meta->pTagIdx, newTagIdxKey, newTagIdxKeySize, NULL, 0, meta->txn) != 0) {
        TSDB_CHECK_CODE(code = TSDB_CODE_TDB_OP_ERROR, lino, _exit);
      }
    }
  }

  if (isTagChanged) {
    code = metaUpsertCtbIdx(meta, newChildTableEntry);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  metaTagIdxKeyDestroy(newTagIdxKey);
  metaTagIdxKeyDestroy(oldTagIdxKey);
  return code;
}

static int32_t metaHandleChildTableJsonTagUpdate(SMeta *meta, const SMetaEntry *oldChildTableEntry,
                                                 const SMetaEntry *newChildTableEntry,
                                                 const SMetaEntry *superTableEntry) {
  int32_t code = 0;
  int32_t lino = 0;

  bool isTagChanged = true;  // TODO

  if (isTagChanged) {
    code = metaUpsertTagIdx(meta, newChildTableEntry, superTableEntry);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = metaUpsertCtbIdx(meta, newChildTableEntry);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaHandleChildTableTagUpdate(SMeta *meta, const SMetaEntry *oldChildTableEntry,
                                             const SMetaEntry *newChildTableEntry, const SMetaEntry *superTableEntry) {
  int32_t code = 0;
  int32_t lino = 0;

  const SSchemaWrapper *tagSchema = &superTableEntry->stbEntry.schemaTag;

  if (tagSchema->nCols == 1 && tagSchema->pSchema[0].type == TSDB_DATA_TYPE_JSON) {
    code = metaHandleChildTableJsonTagUpdate(meta, oldChildTableEntry, newChildTableEntry, superTableEntry);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    code = metaHandleChildTableNormalTagUpdate(meta, oldChildTableEntry, newChildTableEntry, superTableEntry);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaHandleChildTableTTLUpdate(SMeta *meta, const SMetaEntry *oldChildTableEntry,
                                             const SMetaEntry *newChildTableEntry) {
  int32_t code = 0;
  int32_t lino = 0;

  if (oldChildTableEntry->ctbEntry.ttlDays != newChildTableEntry->ctbEntry.ttlDays) {
    code = metaDropTtlIdx(meta, oldChildTableEntry);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = metaUpsertTtlIdx(meta, newChildTableEntry);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaHandleChildTableEntryUpdate(SMeta *meta, const SMetaEntry *newChildTableEntry,
                                               const SMetaEntry *oldChildTableEntry) {
  int32_t     code = 0;
  int32_t     lino = 0;
  SMetaEntry *superTableEntry = NULL;

  ASSERT(newChildTableEntry->ctbEntry.suid == oldChildTableEntry->ctbEntry.suid);

  code = metaGetTableEntryByUidImpl(meta, newChildTableEntry->ctbEntry.suid, &superTableEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = metaHandleChildTableTagUpdate(meta, oldChildTableEntry, newChildTableEntry, superTableEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = metaHandleChildTableTTLUpdate(meta, oldChildTableEntry, newChildTableEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  metaEntryCloneDestroy(superTableEntry);
  return code;
}

static int32_t metaHandleNormalTableTTLUpdate(SMeta *meta, const SMetaEntry *oldNormalTableEntry,
                                              const SMetaEntry *newNormalTableEntry) {
  int32_t code = 0;
  int32_t lino = 0;

  if (oldNormalTableEntry->ntbEntry.ttlDays != newNormalTableEntry->ntbEntry.ttlDays) {
    code = metaDropTtlIdx(meta, oldNormalTableEntry);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = metaUpsertTtlIdx(meta, newNormalTableEntry);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaHandleNormalTableEntryUpdate(SMeta *meta, const SMetaEntry *newNormalTableEntry,
                                                const SMetaEntry *oldNormalTableEntry) {
  int32_t code = 0;
  int32_t lino = 0;

  if (newNormalTableEntry->ntbEntry.schemaRow.version !=
      oldNormalTableEntry->ntbEntry.schemaRow.version) {  // schema changed
    ASSERT(newNormalTableEntry->ntbEntry.schemaRow.version == oldNormalTableEntry->ntbEntry.schemaRow.version + 1);

    /* schema.db */
    code = metaUpsertSchema(meta, newNormalTableEntry);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (newNormalTableEntry->ntbEntry.schemaRow.nCols <
        oldNormalTableEntry->ntbEntry.schemaRow.nCols) {  // delete a column
      // TODO: deal with cache
    } else if (newNormalTableEntry->ntbEntry.schemaRow.nCols >
               oldNormalTableEntry->ntbEntry.schemaRow.nCols) {  // add a column
      // TODO: deal with cache
    }

    meta->pVnode->config.vndStats.numOfNTimeSeries +=
        (newNormalTableEntry->ntbEntry.schemaRow.nCols - oldNormalTableEntry->ntbEntry.schemaRow.nCols);
  }

  code = metaHandleNormalTableTTLUpdate(meta, oldNormalTableEntry, newNormalTableEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaHandleEntryUpdate(SMeta *meta, const SMetaEntry *newEntry) {
  int32_t code = 0;
  int32_t lino = 0;

  SMetaEntry *oldEntry = NULL;

  code = metaGetTableEntryByUidImpl(meta, newEntry->uid, &oldEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  ASSERT(oldEntry->type == newEntry->type);
  ASSERT(oldEntry->version < newEntry->version);

  if (newEntry->type == TSDB_SUPER_TABLE) {
    code = metaHandleSuperTableEntryUpdate(meta, newEntry, oldEntry);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else if (newEntry->type == TSDB_CHILD_TABLE) {
    code = metaHandleChildTableEntryUpdate(meta, newEntry, oldEntry);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else if (newEntry->type == TSDB_NORMAL_TABLE) {
    code = metaHandleNormalTableEntryUpdate(meta, newEntry, oldEntry);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    ASSERT(0);
  }

  /* uid.idx */
  code = metaUpsertUidIdx(meta, newEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  metaEntryCloneDestroy(oldEntry);
  return code;
}

static int32_t metaHandleSuperTableInsert(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

  /* schema.db */
  code = metaUpsertSchema(meta, entry);
  TSDB_CHECK_CODE(code, lino, _exit);

  /* suid.idx */
  code = metaUpsertSuidIdx(meta, entry);
  TSDB_CHECK_CODE(code, lino, _exit);

  // increase super table count
  meta->pVnode->config.vndStats.numOfSTables++;

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaHandleNormalTableInsert(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

  /* schema.db */
  code = metaUpsertSchema(meta, entry);
  TSDB_CHECK_CODE(code, lino, _exit);

  /* btime.idx */
  code = metaUpsertBtimeIdx(meta, entry);
  TSDB_CHECK_CODE(code, lino, _exit);

  // code = metaUpdateNcolIdx(meta, entry);
  // TSDB_CHECK_CODE(code, lino, _exit);

  // code = metaUpdateTtl(meta, entry);
  // TSDB_CHECK_CODE(code, lino, _exit);

  meta->pVnode->config.vndStats.numOfNTables++;
  meta->pVnode->config.vndStats.numOfNTimeSeries += (entry->ntbEntry.schemaRow.nCols - 1);

#if 0
  metaTimeSeriesNotifyCheck(pMeta);
  if (!TSDB_CACHE_NO(pMeta->pVnode->config)) {
    tsdbCacheNewTable(pMeta->pVnode->pTsdb, me.uid, -1, &me.ntbEntry.schemaRow);
  }
#endif

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaHandleChildTableInsert(SMeta *meta, const SMetaEntry *childTableEntry) {
  int32_t code = 0;
  int32_t lino = 0;

  SMetaEntry *superTableEntry = NULL;
  code = metaGetTableEntryByUidImpl(meta, childTableEntry->ctbEntry.suid, &superTableEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  /* ctb.idx */
  code = metaUpsertCtbIdx(meta, childTableEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  /* tag.idx */
  code = metaUpsertTagIdx(meta, childTableEntry, superTableEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  /* btime.idx */
  code = metaUpsertBtimeIdx(meta, childTableEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  /* ttl.idx */
  code = metaUpsertTtlIdx(meta, childTableEntry);
  TSDB_CHECK_CODE(code, lino, _exit);

  /* opther operations */
  meta->pVnode->config.vndStats.numOfCTables++;
  if (!metaTbInFilterCache(meta, superTableEntry->name, 1)) {
    meta->pVnode->config.vndStats.numOfTimeSeries += (superTableEntry->stbEntry.schemaRow.nCols - 1);
  }
  metaUpdateStbStats(meta, superTableEntry->uid, 1, 0);
  metaUidCacheClear(meta, superTableEntry->uid);
  metaTbGroupCacheClear(meta, superTableEntry->uid);

#if 0
  metaTimeSeriesNotifyCheck(meta);
  if (!TSDB_CACHE_NO(meta->pVnode->config)) {
    tsdbCacheNewTable(meta->pVnode->pTsdb, me.uid, me.ctbEntry.suid, NULL);
  }
#endif

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  metaEntryCloneDestroy(superTableEntry);
  return code;
}

static int32_t metaHandleEntryInsert(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

  /* uid.idx */
  code = metaUpsertUidIdx(meta, entry);
  TSDB_CHECK_CODE(code, lino, _exit);

  /* name.idx */
  code = metaUpsertNameIdx(meta, entry);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (entry->type == TSDB_SUPER_TABLE) {
    code = metaHandleSuperTableInsert(meta, entry);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else if (entry->type == TSDB_CHILD_TABLE) {
    code = metaHandleChildTableInsert(meta, entry);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else if (entry->type == TSDB_NORMAL_TABLE) {
    code = metaHandleNormalTableInsert(meta, entry);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    ASSERT(0);
  }

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t metaHandleEntry(SMeta *meta, const SMetaEntry *entry) {
  int32_t     code = 0;
  int32_t     lino = 0;
  const char *name;

  metaWLock(meta);

  if (entry->type < 0) {
    // drop
    name = NULL;
    code = metadHanleEntryDelete(meta, entry);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    name = entry->name;

    if (tdbTbGet(meta->pUidIdx, &entry->uid, sizeof(entry->uid), NULL, NULL) == 0) {
      // update
      code = metaHandleEntryUpdate(meta, entry);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else {
      // create
      code = metaHandleEntryInsert(meta, entry);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  /* table.db */
  code = metaUpsertTableEntry(meta, entry);
  TSDB_CHECK_CODE(code, lino, _exit);

  meta->changed = true;

_exit:
  metaULock(meta);
  if (code) {
    metaError("vgId:%d handle meta entry failed at line:%d since %s, version:%" PRId64 ", type:%d, uid:%" PRId64
              ", name:%s",
              TD_VID(meta->pVnode), lino, tstrerror(code), entry->version, entry->type, entry->uid, name);
  } else {
    metaDebug("vgId:%d handle meta entry success, version:%" PRId64 ", type:%d, uid:%" PRId64 ", name:%s",
              TD_VID(meta->pVnode), entry->version, entry->type, entry->uid, name);
  }
  return (terrno = code);
}