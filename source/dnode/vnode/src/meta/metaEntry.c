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

// =======================
/* table.db */
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

static int32_t metadHanleEntryDelete(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

  // TODO

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaHandleEntryUpdate(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

  // TODO

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
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

  // TODO
  // code = metaUpdateNcolIdx(meta, entry);
  // TSDB_CHECK_CODE(code, lino, _exit);

  // code = metaUpdateTtl(meta, entry);
  // TSDB_CHECK_CODE(code, lino, _exit);

  meta->pVnode->config.vndStats.numOfNTables++;
  meta->pVnode->config.vndStats.numOfNTimeSeries += entry->ntbEntry.schemaRow.nCols - 1;

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaHandleChildTableInsert(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

  // TODO
  meta->pVnode->config.vndStats.numOfCTables++;

_exit:
  if (code) {
    metaError("vgId:%d %s failed at line %d since %s", TD_VID(meta->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t metaHandleEntryInsert(SMeta *meta, const SMetaEntry *entry) {
  int32_t code = 0;
  int32_t lino = 0;

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

  /* table.db */
  code = metaUpsertTableEntry(meta, entry);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (entry->type < 0) {  // delete
    name = NULL;
    code = metadHanleEntryDelete(meta, entry);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    bool isExist = false;
    name = entry->name;

    if (tdbTbGet(meta->pUidIdx, &entry->uid, sizeof(entry->uid), NULL, NULL) == 0) {
      isExist = true;
    }

    /* uid.idx */
    code = metaUpsertUidIdx(meta, entry);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (isExist) {  // update
      code = metaHandleEntryUpdate(meta, entry);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else {  // insert
      code = metaHandleEntryInsert(meta, entry);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

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