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

static int metaSaveToTbDb(SMeta *pMeta, int64_t version, const SMetaEntry *pME);
static int metaUpdateUidIdx(SMeta *pMeta, tb_uid_t uid, int64_t version);
static int metaUpdateNameIdx(SMeta *pMeta, const char *name, tb_uid_t uid);

int metaCreateSTable(SMeta *pMeta, int64_t version, SVCreateStbReq *pReq) {
  SSkmDbKey   skmDbKey = {0};
  SMetaEntry  me = {0};
  int         kLen = 0;
  int         vLen = 0;
  const void *pKey = NULL;
  const void *pVal = NULL;
  void       *pBuf = NULL;
  int32_t     szBuf = 0;
  void       *p = NULL;
  SCoder      coder = {0};

  {
    // TODO: validate request (uid and name unique)
  }

  // set structs
  me.type = TSDB_SUPER_TABLE;
  me.uid = pReq->suid;
  me.name = pReq->name;
  me.stbEntry.nCols = pReq->nCols;
  me.stbEntry.sver = pReq->sver;
  me.stbEntry.pSchema = pReq->pSchema;
  me.stbEntry.nTags = pReq->nTags;
  me.stbEntry.pSchemaTg = pReq->pSchemaTg;

  skmDbKey.uid = pReq->suid;
  skmDbKey.sver = 0;  // (TODO)

  // save to table.db
  if (metaSaveToTbDb(pMeta, version, &me) < 0) goto _err;

  // update uid idx
  if (metaUpdateUidIdx(pMeta, me.uid, version) < 0) goto _err;

  // update name.idx
  if (metaUpdateNameIdx(pMeta, me.name, me.uid) < 0) goto _err;

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

int metaCreateTable(SMeta *pMeta, STbCfg *pTbCfg) {
#if 0
  if (metaSaveTableToDB(pMeta, pTbCfg) < 0) {
    // TODO: handle error
    return -1;
  }

  if (metaSaveTableToIdx(pMeta, pTbCfg) < 0) {
    // TODO: handle error
    return -1;
  }
#endif

  return 0;
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

static int metaSaveToTbDb(SMeta *pMeta, int64_t version, const SMetaEntry *pME) {
  void  *pKey = NULL;
  void  *pVal = NULL;
  int    kLen = 0;
  int    vLen = 0;
  SCoder coder = {0};

  // set key and value
  pKey = &version;
  kLen = sizeof(version);

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
  if (tdbDbInsert(pMeta->pTbDb, pKey, kLen, pVal, vLen, NULL) < 0) {
    goto _err;
  }

  taosMemoryFree(pVal);
  return 0;

_err:
  taosMemoryFree(pVal);
  return -1;
}

static int metaUpdateUidIdx(SMeta *pMeta, tb_uid_t uid, int64_t version) {
  return tdbDbInsert(pMeta->pUidIdx, &uid, sizeof(uid), &version, sizeof(version), NULL);
}

static int metaUpdateNameIdx(SMeta *pMeta, const char *name, tb_uid_t uid) {
  return tdbDbInsert(pMeta->pNameIdx, name, strlen(name) + 1, &uid, sizeof(uid), NULL);
}