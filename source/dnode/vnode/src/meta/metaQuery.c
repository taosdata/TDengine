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

void metaReaderInit(SMetaReader *pReader, SMeta *pMeta, int32_t flags) {
  memset(pReader, 0, sizeof(*pReader));
  pReader->flags = flags;
  pReader->pMeta = pMeta;
  metaRLock(pMeta);
}

void metaReaderClear(SMetaReader *pReader) {
  if (pReader->pMeta) {
    metaULock(pReader->pMeta);
  }
  tDecoderClear(&pReader->coder);
  tdbFree(pReader->pBuf);
}

int metaGetTableEntryByVersion(SMetaReader *pReader, int64_t version, tb_uid_t uid) {
  SMeta   *pMeta = pReader->pMeta;
  STbDbKey tbDbKey = {.version = version, .uid = uid};

  // query table.db
  if (tdbDbGet(pMeta->pTbDb, &tbDbKey, sizeof(tbDbKey), &pReader->pBuf, &pReader->szBuf) < 0) {
    terrno = TSDB_CODE_PAR_TABLE_NOT_EXIST;
    goto _err;
  }

  // decode the entry
  tDecoderInit(&pReader->coder, pReader->pBuf, pReader->szBuf);

  if (metaDecodeEntry(&pReader->coder, &pReader->me) < 0) {
    goto _err;
  }

  return 0;

_err:
  return -1;
}

int metaGetTableEntryByUid(SMetaReader *pReader, tb_uid_t uid) {
  SMeta  *pMeta = pReader->pMeta;
  int64_t version;

  // query uid.idx
  if (tdbDbGet(pMeta->pUidIdx, &uid, sizeof(uid), &pReader->pBuf, &pReader->szBuf) < 0) {
    terrno = TSDB_CODE_PAR_TABLE_NOT_EXIST;
    return -1;
  }

  version = *(int64_t *)pReader->pBuf;
  return metaGetTableEntryByVersion(pReader, version, uid);
}

int metaGetTableEntryByName(SMetaReader *pReader, const char *name) {
  SMeta   *pMeta = pReader->pMeta;
  tb_uid_t uid;

  // query name.idx
  if (tdbDbGet(pMeta->pNameIdx, name, strlen(name) + 1, &pReader->pBuf, &pReader->szBuf) < 0) {
    terrno = TSDB_CODE_PAR_TABLE_NOT_EXIST;
    return -1;
  }

  uid = *(tb_uid_t *)pReader->pBuf;
  return metaGetTableEntryByUid(pReader, uid);
}

int metaReadNext(SMetaReader *pReader) {
  SMeta *pMeta = pReader->pMeta;

  // TODO

  return 0;
}

#if 1  // ===================================================
SMTbCursor *metaOpenTbCursor(SMeta *pMeta) {
  SMTbCursor *pTbCur = NULL;

  pTbCur = (SMTbCursor *)taosMemoryCalloc(1, sizeof(*pTbCur));
  if (pTbCur == NULL) {
    return NULL;
  }

  metaReaderInit(&pTbCur->mr, pMeta, 0);

  tdbDbcOpen(pMeta->pUidIdx, &pTbCur->pDbc, NULL);

  tdbDbcMoveToFirst(pTbCur->pDbc);

  return pTbCur;
}

void metaCloseTbCursor(SMTbCursor *pTbCur) {
  if (pTbCur) {
    tdbFree(pTbCur->pKey);
    tdbFree(pTbCur->pVal);
    metaReaderClear(&pTbCur->mr);
    if (pTbCur->pDbc) {
      tdbDbcClose(pTbCur->pDbc);
    }
    taosMemoryFree(pTbCur);
  }
}

int metaTbCursorNext(SMTbCursor *pTbCur) {
  int    ret;
  void  *pBuf;
  STbCfg tbCfg;

  for (;;) {
    ret = tdbDbcNext(pTbCur->pDbc, &pTbCur->pKey, &pTbCur->kLen, &pTbCur->pVal, &pTbCur->vLen);
    if (ret < 0) {
      return -1;
    }

    metaGetTableEntryByVersion(&pTbCur->mr, *(int64_t *)pTbCur->pVal, *(tb_uid_t *)pTbCur->pKey);
    if (pTbCur->mr.me.type == TSDB_SUPER_TABLE) {
      continue;
    }

    break;
  }

  return 0;
}

SSchemaWrapper *metaGetTableSchema(SMeta *pMeta, tb_uid_t uid, int32_t sver, bool isinline) {
  void           *pKey = NULL;
  void           *pVal = NULL;
  int             kLen = 0;
  int             vLen = 0;
  int             ret;
  SSkmDbKey       skmDbKey;
  SSchemaWrapper *pSW = NULL;
  SSchema        *pSchema = NULL;
  void           *pBuf;
  SDecoder        coder = {0};

  // fetch
  skmDbKey.uid = uid;
  skmDbKey.sver = sver;
  pKey = &skmDbKey;
  kLen = sizeof(skmDbKey);
  metaRLock(pMeta);
  ret = tdbDbGet(pMeta->pSkmDb, pKey, kLen, &pVal, &vLen);
  metaULock(pMeta);
  if (ret < 0) {
    return NULL;
  }

  // decode
  pBuf = pVal;
  pSW = taosMemoryMalloc(sizeof(SSchemaWrapper));

  tDecoderInit(&coder, pVal, vLen);
  tDecodeSSchemaWrapper(&coder, pSW);
  pSchema = taosMemoryMalloc(sizeof(SSchema) * pSW->nCols);
  memcpy(pSchema, pSW->pSchema, sizeof(SSchema) * pSW->nCols);
  tDecoderClear(&coder);

  pSW->pSchema = pSchema;

  tdbFree(pVal);

  return pSW;
}

struct SMCtbCursor {
  SMeta   *pMeta;
  TDBC    *pCur;
  tb_uid_t suid;
  void    *pKey;
  void    *pVal;
  int      kLen;
  int      vLen;
};

SMCtbCursor *metaOpenCtbCursor(SMeta *pMeta, tb_uid_t uid) {
  SMCtbCursor *pCtbCur = NULL;
  SCtbIdxKey   ctbIdxKey;
  int          ret;
  int          c;

  pCtbCur = (SMCtbCursor *)taosMemoryCalloc(1, sizeof(*pCtbCur));
  if (pCtbCur == NULL) {
    return NULL;
  }

  pCtbCur->pMeta = pMeta;
  pCtbCur->suid = uid;
  metaRLock(pMeta);

  ret = tdbDbcOpen(pMeta->pCtbIdx, &pCtbCur->pCur, NULL);
  if (ret < 0) {
    metaULock(pMeta);
    taosMemoryFree(pCtbCur);
    return NULL;
  }

  // move to the suid
  ctbIdxKey.suid = uid;
  ctbIdxKey.uid = INT64_MIN;
  tdbDbcMoveTo(pCtbCur->pCur, &ctbIdxKey, sizeof(ctbIdxKey), &c);
  if (c > 0) {
    tdbDbcMoveToNext(pCtbCur->pCur);
  }

  return pCtbCur;
}

void metaCloseCtbCurosr(SMCtbCursor *pCtbCur) {
  if (pCtbCur) {
    if (pCtbCur->pMeta) metaULock(pCtbCur->pMeta);
    if (pCtbCur->pCur) {
      tdbDbcClose(pCtbCur->pCur);

      tdbFree(pCtbCur->pKey);
      tdbFree(pCtbCur->pVal);
    }

    taosMemoryFree(pCtbCur);
  }
}

tb_uid_t metaCtbCursorNext(SMCtbCursor *pCtbCur) {
  int         ret;
  SCtbIdxKey *pCtbIdxKey;

  ret = tdbDbcNext(pCtbCur->pCur, &pCtbCur->pKey, &pCtbCur->kLen, &pCtbCur->pVal, &pCtbCur->vLen);
  if (ret < 0) {
    return 0;
  }

  pCtbIdxKey = pCtbCur->pKey;
  if (pCtbIdxKey->suid > pCtbCur->suid) {
    return 0;
  }

  return pCtbIdxKey->uid;
}

STSchema *metaGetTbTSchema(SMeta *pMeta, tb_uid_t uid, int32_t sver) {
  tb_uid_t        quid;
  SMetaReader     mr = {0};
  STSchema       *pTSchema = NULL;
  SSchemaWrapper *pSW = NULL;
  STSchemaBuilder sb = {0};
  SSchema        *pSchema;

  metaReaderInit(&mr, pMeta, 0);
  metaGetTableEntryByUid(&mr, uid);

  if (mr.me.type == TSDB_CHILD_TABLE) {
    quid = mr.me.ctbEntry.suid;
  } else {
    quid = uid;
  }

  metaReaderClear(&mr);

  pSW = metaGetTableSchema(pMeta, quid, sver, 0);
  if (!pSW) return NULL;

  tdInitTSchemaBuilder(&sb, 0);
  for (int i = 0; i < pSW->nCols; i++) {
    pSchema = pSW->pSchema + i;
    tdAddColToSchema(&sb, pSchema->type, pSchema->flags, pSchema->colId, pSchema->bytes);
  }
  pTSchema = tdGetSchemaFromBuilder(&sb);
  tdDestroyTSchemaBuilder(&sb);

  taosMemoryFree(pSW->pSchema);
  taosMemoryFree(pSW);
  return pTSchema;
}

STSmaWrapper *metaGetSmaInfoByTable(SMeta *pMeta, tb_uid_t uid) {
#if 0
#ifdef META_TDB_SMA_TEST
  STSmaWrapper *pSW = NULL;

  pSW = taosMemoryCalloc(1, sizeof(*pSW));
  if (pSW == NULL) {
    return NULL;
  }

  SMSmaCursor *pCur = metaOpenSmaCursor(pMeta, uid);
  if (pCur == NULL) {
    taosMemoryFree(pSW);
    return NULL;
  }

  void       *pBuf = NULL;
  SSmaIdxKey *pSmaIdxKey = NULL;

  while (true) {
    // TODO: lock during iterate?
    if (tdbDbcNext(pCur->pCur, &pCur->pKey, &pCur->kLen, NULL, &pCur->vLen) == 0) {
      pSmaIdxKey = pCur->pKey;
      ASSERT(pSmaIdxKey != NULL);

      void *pSmaVal = metaGetSmaInfoByIndex(pMeta, pSmaIdxKey->smaUid, false);

      if (pSmaVal == NULL) {
        tsdbWarn("no tsma exists for indexUid: %" PRIi64, pSmaIdxKey->smaUid);
        continue;
      }

      ++pSW->number;
      STSma *tptr = (STSma *)taosMemoryRealloc(pSW->tSma, pSW->number * sizeof(STSma));
      if (tptr == NULL) {
        tdbFree(pSmaVal);
        metaCloseSmaCursor(pCur);
        tdDestroyTSmaWrapper(pSW);
        taosMemoryFreeClear(pSW);
        return NULL;
      }
      pSW->tSma = tptr;
      pBuf = pSmaVal;
      if (tDecodeTSma(pBuf, pSW->tSma + pSW->number - 1) == NULL) {
        tdbFree(pSmaVal);
        metaCloseSmaCursor(pCur);
        tdDestroyTSmaWrapper(pSW);
        taosMemoryFreeClear(pSW);
        return NULL;
      }
      tdbFree(pSmaVal);
      continue;
    }
    break;
  }

  metaCloseSmaCursor(pCur);

  return pSW;

#endif
#endif
  return NULL;
}

int metaGetTbNum(SMeta *pMeta) {
  // TODO
  // ASSERT(0);
  return 0;
}

SArray *metaGetSmaTbUids(SMeta *pMeta, bool isDup) {
#if 0
  // TODO
  // ASSERT(0); // comment this line to pass CI
  // return NULL:
#ifdef META_TDB_SMA_TEST
  SArray  *pUids = NULL;
  SMetaDB *pDB = pMeta->pDB;
  void    *pKey;

  // TODO: lock?
  SMSmaCursor *pCur = metaOpenSmaCursor(pMeta, 0);
  if (pCur == NULL) {
    return NULL;
  }
  // TODO: lock?

  SSmaIdxKey *pSmaIdxKey = NULL;
  tb_uid_t    uid = 0;
  while (true) {
    // TODO: lock during iterate?
    if (tdbDbcNext(pCur->pCur, &pCur->pKey, &pCur->kLen, NULL, &pCur->vLen) == 0) {
      ASSERT(pSmaIdxKey != NULL);
      pSmaIdxKey = pCur->pKey;

      if (pSmaIdxKey->uid == 0 || pSmaIdxKey->uid == uid) {
        continue;
      }
      uid = pSmaIdxKey->uid;

      if (!pUids) {
        pUids = taosArrayInit(16, sizeof(tb_uid_t));
        if (!pUids) {
          metaCloseSmaCursor(pCur);
          return NULL;
        }
      }

      taosArrayPush(pUids, &uid);

      continue;
    }
    break;
  }

  metaCloseSmaCursor(pCur);

  return pUids;
#endif
#endif
  return NULL;
}

void *metaGetSmaInfoByIndex(SMeta *pMeta, int64_t indexUid, bool isDecode) {
#if 0
  // TODO
  // ASSERT(0);
  // return NULL;
#ifdef META_TDB_SMA_TEST
  SMetaDB *pDB = pMeta->pDB;
  void    *pKey = NULL;
  void    *pVal = NULL;
  int      kLen = 0;
  int      vLen = 0;
  int      ret = -1;

  // Set key
  pKey = (void *)&indexUid;
  kLen = sizeof(indexUid);

  // Query
  ret = tdbDbGet(pDB->pSmaDB, pKey, kLen, &pVal, &vLen);
  if (ret != 0 || !pVal) {
    return NULL;
  }

  if (!isDecode) {
    // return raw value
    return pVal;
  }

  // Decode
  STSma *pCfg = (STSma *)taosMemoryCalloc(1, sizeof(STSma));
  if (pCfg == NULL) {
    taosMemoryFree(pVal);
    return NULL;
  }

  void *pBuf = pVal;
  if (tDecodeTSma(pBuf, pCfg) == NULL) {
    tdDestroyTSma(pCfg);
    taosMemoryFree(pCfg);
    tdbFree(pVal);
    return NULL;
  }

  tdbFree(pVal);
  return pCfg;
#endif
#endif
  return NULL;
}

#endif

const void *metaGetTableTagVal(SMetaEntry *pEntry, int16_t cid) {
  ASSERT(pEntry->type == TSDB_CHILD_TABLE);
  return tdGetKVRowValOfCol((const SKVRow)pEntry->ctbEntry.pTags, cid);
}