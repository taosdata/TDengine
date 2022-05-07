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

  metaDebug("vgId:%d super table is created, name:%s uid: %" PRId64, TD_VID(pMeta->pVnode), pReq->name, pReq->suid);

  return 0;

_err:
  metaError("vgId:%d failed to create super table: %s uid: %" PRId64 " since %s", TD_VID(pMeta->pVnode), pReq->name,
            pReq->suid, tstrerror(terrno));
  return -1;
}

int metaDropSTable(SMeta *pMeta, int64_t verison, SVDropStbReq *pReq) {
  TDBC       *pNameIdxc = NULL;
  TDBC       *pUidIdxc = NULL;
  TDBC       *pCtbIdxc = NULL;
  SCtbIdxKey *pCtbIdxKey;
  const void *pKey = NULL;
  int         nKey;
  const void *pData = NULL;
  int         nData;
  int         c, ret;

  // prepare uid idx cursor
  tdbDbcOpen(pMeta->pUidIdx, &pUidIdxc, &pMeta->txn);
  ret = tdbDbcMoveTo(pUidIdxc, &pReq->suid, sizeof(tb_uid_t), &c);
  if (ret < 0 || c != 0) {
    terrno = TSDB_CODE_VND_TB_NOT_EXIST;
    tdbDbcClose(pUidIdxc);
    goto _err;
  }

  // prepare name idx cursor
  tdbDbcOpen(pMeta->pNameIdx, &pNameIdxc, &pMeta->txn);
  ret = tdbDbcMoveTo(pNameIdxc, pReq->name, strlen(pReq->name) + 1, &c);
  if (ret < 0 || c != 0) {
    ASSERT(0);
  }

  tdbDbcDelete(pUidIdxc);
  tdbDbcDelete(pNameIdxc);
  tdbDbcClose(pUidIdxc);
  tdbDbcClose(pNameIdxc);

  // loop to drop each child table
  tdbDbcOpen(pMeta->pCtbIdx, &pCtbIdxc, &pMeta->txn);
  ret = tdbDbcMoveTo(pCtbIdxc, &(SCtbIdxKey){.suid = pReq->suid, .uid = INT64_MIN}, sizeof(SCtbIdxKey), &c);
  if (ret < 0 || (c < 0 && tdbDbcMoveToNext(pCtbIdxc) < 0)) {
    tdbDbcClose(pCtbIdxc);
    goto _exit;
  }

  for (;;) {
    tdbDbcGet(pCtbIdxc, &pKey, &nKey, NULL, NULL);
    pCtbIdxKey = (SCtbIdxKey *)pKey;

    if (pCtbIdxKey->suid > pReq->suid) break;

    // drop the child table (TODO)

    if (tdbDbcMoveToNext(pCtbIdxc) < 0) break;
  }

_exit:
  metaDebug("vgId:%d  super table %s uid:%" PRId64 " is dropped", TD_VID(pMeta->pVnode), pReq->name, pReq->suid);
  return 0;

_err:
  metaError("vgId:%d failed to drop super table %s uid:%" PRId64 " since %s", TD_VID(pMeta->pVnode), pReq->name,
            pReq->suid, tstrerror(terrno));
  return -1;
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

int metaDropTable(SMeta *pMeta, int64_t version, SVDropTbReq *pReq) {
  TDBC       *pTbDbc = NULL;
  TDBC       *pUidIdxc = NULL;
  TDBC       *pNameIdxc = NULL;
  const void *pData;
  int         nData;
  tb_uid_t    uid;
  int64_t     tver;
  SMetaEntry  me = {0};
  SCoder      coder = {0};
  int8_t      type;
  int64_t     ctime;
  tb_uid_t    suid;
  int         c = 0, ret;

  // search & delete the name idx
  tdbDbcOpen(pMeta->pNameIdx, &pNameIdxc, &pMeta->txn);
  ret = tdbDbcMoveTo(pNameIdxc, pReq->name, strlen(pReq->name) + 1, &c);
  if (ret < 0 || !tdbDbcIsValid(pNameIdxc) || c) {
    tdbDbcClose(pNameIdxc);
    terrno = TSDB_CODE_VND_TABLE_NOT_EXIST;
    return -1;
  }

  ret = tdbDbcGet(pNameIdxc, NULL, NULL, &pData, &nData);
  if (ret < 0) {
    ASSERT(0);
    return -1;
  }

  uid = *(tb_uid_t *)pData;

  tdbDbcDelete(pNameIdxc);
  tdbDbcClose(pNameIdxc);

  // search & delete uid idx
  tdbDbcOpen(pMeta->pUidIdx, &pUidIdxc, &pMeta->txn);
  ret = tdbDbcMoveTo(pUidIdxc, &uid, sizeof(uid), &c);
  if (ret < 0 || c != 0) {
    ASSERT(0);
    return -1;
  }

  ret = tdbDbcGet(pUidIdxc, NULL, NULL, &pData, &nData);
  if (ret < 0) {
    ASSERT(0);
    return -1;
  }

  tver = *(int64_t *)pData;
  tdbDbcDelete(pUidIdxc);
  tdbDbcClose(pUidIdxc);

  // search and get meta entry
  tdbDbcOpen(pMeta->pTbDb, &pTbDbc, &pMeta->txn);
  ret = tdbDbcMoveTo(pTbDbc, &(STbDbKey){.uid = uid, .version = tver}, sizeof(STbDbKey), &c);
  if (ret < 0 || c != 0) {
    ASSERT(0);
    return -1;
  }

  ret = tdbDbcGet(pTbDbc, NULL, NULL, &pData, &nData);
  if (ret < 0) {
    ASSERT(0);
    return -1;
  }

  // decode entry
  void *pDataCopy = taosMemoryMalloc(nData);  // remove the copy (todo)
  memcpy(pDataCopy, pData, nData);
  tCoderInit(&coder, TD_LITTLE_ENDIAN, pDataCopy, nData, TD_DECODER);
  ret = metaDecodeEntry(&coder, &me);
  if (ret < 0) {
    ASSERT(0);
    return -1;
  }

  type = me.type;
  if (type == TSDB_CHILD_TABLE) {
    ctime = me.ctbEntry.ctime;
    suid = me.ctbEntry.suid;
  } else if (type == TSDB_NORMAL_TABLE) {
    ctime = me.ntbEntry.ctime;
    suid = 0;
  } else {
    ASSERT(0);
  }

  taosMemoryFree(pDataCopy);
  tCoderClear(&coder);
  tdbDbcClose(pTbDbc);

  if (type == TSDB_CHILD_TABLE) {
    // remove the pCtbIdx
    TDBC *pCtbIdxc = NULL;
    tdbDbcOpen(pMeta->pCtbIdx, &pCtbIdxc, &pMeta->txn);

    ret = tdbDbcMoveTo(pCtbIdxc, &(SCtbIdxKey){.suid = suid, .uid = uid}, sizeof(SCtbIdxKey), &c);
    if (ret < 0 || c != 0) {
      ASSERT(0);
      return -1;
    }

    tdbDbcDelete(pCtbIdxc);
    tdbDbcClose(pCtbIdxc);

    // remove tags from pTagIdx (todo)
  } else if (type == TSDB_NORMAL_TABLE) {
    // remove from pSkmDb
  } else {
    ASSERT(0);
  }

  // remove from ttl (todo)
  if (ctime > 0) {
  }

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

  int32_t ret = 0;
  tEncodeSize(metaEncodeEntry, pME, vLen, ret);
  if (ret < 0) {
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
  int32_t ret = 0;
  tEncodeSize(tEncodeSSchemaWrapper, pSW, vLen, ret);
  if (ret < 0) return -1;
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