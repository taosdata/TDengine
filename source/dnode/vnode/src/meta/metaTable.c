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

static int metaHandleEntry(SMeta *pMeta, const SMetaEntry *pME);
static int metaSaveToTbDb(SMeta *pMeta, const SMetaEntry *pME);
static int metaUpdateUidIdx(SMeta *pMeta, const SMetaEntry *pME);
static int metaUpdateNameIdx(SMeta *pMeta, const SMetaEntry *pME);
static int metaUpdateTtlIdx(SMeta *pMeta, const SMetaEntry *pME);
static int metaSaveToSkmDb(SMeta *pMeta, const SMetaEntry *pME);
static int metaUpdateCtbIdx(SMeta *pMeta, const SMetaEntry *pME);
static int metaUpdateTagIdx(SMeta *pMeta, const SMetaEntry *pME);

int metaCreateSTable(SMeta *pMeta, int64_t version, SVCreateStbReq *pReq) {
  SMetaEntry  me = {0};
  int         kLen = 0;
  int         vLen = 0;
  const void *pKey = NULL;
  const void *pVal = NULL;
  void       *pBuf = NULL;
  int32_t     szBuf = 0;
  void       *p = NULL;
  SCoder      coder = {0};
  SMetaReader mr = {0};

  // validate req
  metaReaderInit(&mr, pMeta, 0);
  if (metaGetTableEntryByName(&mr, pReq->name) == 0) {
// TODO: just for pass case
#if 0
    terrno = TSDB_CODE_TDB_TABLE_ALREADY_EXIST;
    metaReaderClear(&mr);
    return -1;
#else
    metaReaderClear(&mr);
    return 0;
#endif
  }
  metaReaderClear(&mr);

  // set structs
  me.version = version;
  me.type = TSDB_SUPER_TABLE;
  me.uid = pReq->suid;
  me.name = pReq->name;
  me.stbEntry.schema = pReq->schema;
  me.stbEntry.schemaTag = pReq->schemaTag;

  if (metaHandleEntry(pMeta, &me) < 0) goto _err;

  metaDebug("vgId: %d super table is created, name:%s uid: %" PRId64, TD_VID(pMeta->pVnode), pReq->name, pReq->suid);

  return 0;

_err:
  metaError("vgId: %d failed to create super table: %s uid: %" PRId64 " since %s", TD_VID(pMeta->pVnode), pReq->name,
            pReq->suid, tstrerror(terrno));
  return -1;
}

int metaDropSTable(SMeta *pMeta, int64_t verison, SVDropStbReq *pReq) {
  // TODO
  return 0;
}

int metaCreateTable(SMeta *pMeta, int64_t version, SVCreateTbReq *pReq) {
  SMetaEntry  me = {0};
  SMetaReader mr = {0};

  // validate message
  if (pReq->type != TSDB_CHILD_TABLE && pReq->type != TSDB_NORMAL_TABLE) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _err;
  }

  // preprocess req
  pReq->uid = tGenIdPI64();
  pReq->ctime = taosGetTimestampMs();

  // validate req
  metaReaderInit(&mr, pMeta, 0);
  if (metaGetTableEntryByName(&mr, pReq->name) == 0) {
    terrno = TSDB_CODE_TDB_TABLE_ALREADY_EXIST;
    metaReaderClear(&mr);
    return -1;
  }
  metaReaderClear(&mr);

  // build SMetaEntry
  me.version = version;
  me.type = pReq->type;
  me.uid = pReq->uid;
  me.name = pReq->name;
  if (me.type == TSDB_CHILD_TABLE) {
    me.ctbEntry.ctime = pReq->ctime;
    me.ctbEntry.ttlDays = pReq->ttl;
    me.ctbEntry.suid = pReq->ctb.suid;
    me.ctbEntry.pTags = pReq->ctb.pTag;
  } else {
    me.ntbEntry.ctime = pReq->ctime;
    me.ntbEntry.ttlDays = pReq->ttl;
    me.ntbEntry.schema = pReq->ntb.schema;
  }

  if (metaHandleEntry(pMeta, &me) < 0) goto _err;

  metaDebug("vgId:%d table %s uid %" PRId64 " is created, type:%" PRId8, TD_VID(pMeta->pVnode), pReq->name, pReq->uid,
            pReq->type);
  return 0;

_err:
  metaError("vgId:%d failed to create table:%s type:%s since %s", TD_VID(pMeta->pVnode), pReq->name,
            pReq->type == TSDB_CHILD_TABLE ? "child table" : "normal table", tstrerror(terrno));
  return -1;
}

int metaDropTable(SMeta *pMeta, tb_uid_t uid) {
#if 0
  if (metaRemoveTableFromIdx(pMeta, uid) < 0) {
    // TODO: handle error
    return -1;
  }

  if (metaRemoveTableFromIdx(pMeta, uid) < 0) {
    // TODO
    return -1;
  }
#endif

  return 0;
}

static int metaSaveToTbDb(SMeta *pMeta, const SMetaEntry *pME) {
  STbDbKey tbDbKey;
  void    *pKey = NULL;
  void    *pVal = NULL;
  int      kLen = 0;
  int      vLen = 0;
  SCoder   coder = {0};

  // set key and value
  tbDbKey.version = pME->version;
  tbDbKey.uid = pME->uid;

  pKey = &tbDbKey;
  kLen = sizeof(tbDbKey);

  if (tEncodeSize(metaEncodeEntry, pME, vLen) < 0) {
    goto _err;
  }

  pVal = taosMemoryMalloc(vLen);
  if (pVal == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  tCoderInit(&coder, TD_LITTLE_ENDIAN, pVal, vLen, TD_ENCODER);

  if (metaEncodeEntry(&coder, pME) < 0) {
    goto _err;
  }

  tCoderClear(&coder);

  // write to table.db
  if (tdbDbInsert(pMeta->pTbDb, pKey, kLen, pVal, vLen, &pMeta->txn) < 0) {
    goto _err;
  }

  taosMemoryFree(pVal);
  return 0;

_err:
  taosMemoryFree(pVal);
  return -1;
}

static int metaUpdateUidIdx(SMeta *pMeta, const SMetaEntry *pME) {
  return tdbDbInsert(pMeta->pUidIdx, &pME->uid, sizeof(tb_uid_t), &pME->version, sizeof(int64_t), &pMeta->txn);
}

static int metaUpdateNameIdx(SMeta *pMeta, const SMetaEntry *pME) {
  return tdbDbInsert(pMeta->pNameIdx, pME->name, strlen(pME->name) + 1, &pME->uid, sizeof(tb_uid_t), &pMeta->txn);
}

static int metaUpdateTtlIdx(SMeta *pMeta, const SMetaEntry *pME) {
  int32_t    ttlDays;
  int64_t    ctime;
  STtlIdxKey ttlKey;

  if (pME->type == TSDB_CHILD_TABLE) {
    ctime = pME->ctbEntry.ctime;
    ttlDays = pME->ctbEntry.ttlDays;
  } else if (pME->type == TSDB_NORMAL_TABLE) {
    ctime = pME->ntbEntry.ctime;
    ttlDays = pME->ntbEntry.ttlDays;
  } else {
    ASSERT(0);
  }

  if (ttlDays <= 0) return 0;

  ttlKey.dtime = ctime + ttlDays * 24 * 60 * 60;
  ttlKey.uid = pME->uid;

  return tdbDbInsert(pMeta->pTtlIdx, &ttlKey, sizeof(ttlKey), NULL, 0, &pMeta->txn);
}

static int metaUpdateCtbIdx(SMeta *pMeta, const SMetaEntry *pME) {
  SCtbIdxKey ctbIdxKey = {.suid = pME->ctbEntry.suid, .uid = pME->uid};
  return tdbDbInsert(pMeta->pCtbIdx, &ctbIdxKey, sizeof(ctbIdxKey), NULL, 0, &pMeta->txn);
}

static int metaUpdateTagIdx(SMeta *pMeta, const SMetaEntry *pME) {
  // TODO
  return 0;
}

static int metaSaveToSkmDb(SMeta *pMeta, const SMetaEntry *pME) {
  SCoder                coder = {0};
  void                 *pVal = NULL;
  int                   vLen = 0;
  int                   rcode = 0;
  SSkmDbKey             skmDbKey = {0};
  const SSchemaWrapper *pSW;

  if (pME->type == TSDB_SUPER_TABLE) {
    pSW = &pME->stbEntry.schema;
  } else if (pME->type == TSDB_NORMAL_TABLE) {
    pSW = &pME->ntbEntry.schema;
  } else {
    ASSERT(0);
  }

  skmDbKey.uid = pME->uid;
  skmDbKey.sver = pSW->sver;

  // encode schema
  if (tEncodeSize(tEncodeSSchemaWrapper, pSW, vLen) < 0) return -1;
  pVal = taosMemoryMalloc(vLen);
  if (pVal == NULL) {
    rcode = -1;
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  tCoderInit(&coder, TD_LITTLE_ENDIAN, pVal, vLen, TD_ENCODER);
  tEncodeSSchemaWrapper(&coder, pSW);

  if (tdbDbInsert(pMeta->pSkmDb, &skmDbKey, sizeof(skmDbKey), pVal, vLen, &pMeta->txn) < 0) {
    rcode = -1;
    goto _exit;
  }

_exit:
  taosMemoryFree(pVal);
  tCoderClear(&coder);
  return rcode;
}

static int metaHandleEntry(SMeta *pMeta, const SMetaEntry *pME) {
  // save to table.db
  if (metaSaveToTbDb(pMeta, pME) < 0) return -1;

  // update uid.idx
  if (metaUpdateUidIdx(pMeta, pME) < 0) return -1;

  // update name.idx
  if (metaUpdateNameIdx(pMeta, pME) < 0) return -1;

  if (pME->type == TSDB_CHILD_TABLE) {
    // update ctb.idx
    if (metaUpdateCtbIdx(pMeta, pME) < 0) return -1;

    // update tag.idx
    if (metaUpdateTagIdx(pMeta, pME) < 0) return -1;
  } else {
    // update schema.db
    if (metaSaveToSkmDb(pMeta, pME) < 0) return -1;
  }

  if (pME->type != TSDB_SUPER_TABLE) {
    if (metaUpdateTtlIdx(pMeta, pME) < 0) return -1;
  }

  return 0;
}