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
  if (tdbGet(pMeta->pTbDb, &tbDbKey, sizeof(tbDbKey), &pReader->pBuf, &pReader->szBuf) < 0) {
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
  if (tdbGet(pMeta->pUidIdx, &uid, sizeof(uid), &pReader->pBuf, &pReader->szBuf) < 0) {
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
  if (tdbGet(pMeta->pNameIdx, name, strlen(name) + 1, &pReader->pBuf, &pReader->szBuf) < 0) {
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
  ret = tdbGet(pMeta->pSkmDb, pKey, kLen, &pVal, &vLen);
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

void metaCloseCtbCursor(SMCtbCursor *pCtbCur) {
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

int metaGetTbNum(SMeta *pMeta) {
  // TODO
  // ASSERT(0);
  return 0;
}

typedef struct {
  SMeta   *pMeta;
  TDBC    *pCur;
  tb_uid_t uid;
  void    *pKey;
  void    *pVal;
  int      kLen;
  int      vLen;
} SMSmaCursor;

SMSmaCursor *metaOpenSmaCursor(SMeta *pMeta, tb_uid_t uid) {
  SMSmaCursor *pSmaCur = NULL;
  SSmaIdxKey   smaIdxKey;
  int          ret;
  int          c;

  pSmaCur = (SMSmaCursor *)taosMemoryCalloc(1, sizeof(*pSmaCur));
  if (pSmaCur == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pSmaCur->pMeta = pMeta;
  pSmaCur->uid = uid;
  metaRLock(pMeta);

  ret = tdbDbcOpen(pMeta->pSmaIdx, &pSmaCur->pCur, NULL);
  if (ret < 0) {
    metaULock(pMeta);
    taosMemoryFree(pSmaCur);
    return NULL;
  }

  // move to the suid
  smaIdxKey.uid = uid;
  smaIdxKey.smaUid = INT64_MIN;
  tdbDbcMoveTo(pSmaCur->pCur, &smaIdxKey, sizeof(smaIdxKey), &c);
  if (c > 0) {
    tdbDbcMoveToNext(pSmaCur->pCur);
  }

  return pSmaCur;
}

void metaCloseSmaCursor(SMSmaCursor *pSmaCur) {
  if (pSmaCur) {
    if (pSmaCur->pMeta) metaULock(pSmaCur->pMeta);
    if (pSmaCur->pCur) {
      tdbDbcClose(pSmaCur->pCur);

      tdbFree(pSmaCur->pKey);
      tdbFree(pSmaCur->pVal);
    }

    taosMemoryFree(pSmaCur);
  }
}

tb_uid_t metaSmaCursorNext(SMSmaCursor *pSmaCur) {
  int         ret;
  SSmaIdxKey *pSmaIdxKey;

  ret = tdbDbcNext(pSmaCur->pCur, &pSmaCur->pKey, &pSmaCur->kLen, &pSmaCur->pVal, &pSmaCur->vLen);
  if (ret < 0) {
    return 0;
  }

  pSmaIdxKey = pSmaCur->pKey;
  if (pSmaIdxKey->uid > pSmaCur->uid) {
    return 0;
  }

  return pSmaIdxKey->uid;
}

STSmaWrapper *metaGetSmaInfoByTable(SMeta *pMeta, tb_uid_t uid, bool deepCopy) {
  STSmaWrapper *pSW = NULL;
  SArray       *pSmaIds = NULL;

  if (!(pSmaIds = metaGetSmaIdsByTable(pMeta, uid))) {
    return NULL;
  }

  pSW = taosMemoryCalloc(1, sizeof(*pSW));
  if (!pSW) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  pSW->number = taosArrayGetSize(pSmaIds);
  pSW->tSma = taosMemoryCalloc(pSW->number, sizeof(STSma));

  if (!pSW->tSma) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  SMetaReader mr = {0};
  metaReaderInit(&mr, pMeta, 0);
  int64_t smaId;
  int     smaIdx = 0;
  STSma  *pTSma = NULL;
  for (int i = 0; i < pSW->number; ++i) {
    smaId = *(tb_uid_t *)taosArrayGet(pSmaIds, i);
    if (metaGetTableEntryByUid(&mr, smaId) < 0) {
      metaWarn("vgId:%d no entry for tbId: %" PRIi64 ", smaId: %" PRIi64, TD_VID(pMeta->pVnode), uid, smaId);
      continue;
    }
    pTSma = pSW->tSma + smaIdx;
    memcpy(pTSma, mr.me.smaEntry.tsma, sizeof(STSma));
    if (deepCopy) {
      if (pTSma->exprLen > 0) {
        if (!(pTSma->expr = taosMemoryCalloc(1, pTSma->exprLen))) {
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          goto _err;
        }
        memcpy((void *)pTSma->expr, mr.me.smaEntry.tsma->expr, pTSma->exprLen);
      }
      if (pTSma->tagsFilterLen > 0) {
        if (!(pTSma->tagsFilter = taosMemoryCalloc(1, pTSma->tagsFilterLen))) {
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          goto _err;
        }
      }
      memcpy((void *)pTSma->tagsFilter, mr.me.smaEntry.tsma->tagsFilter, pTSma->tagsFilterLen);
    } else {
      pTSma->exprLen = 0;
      pTSma->expr = NULL;
      pTSma->tagsFilterLen = 0;
      pTSma->tagsFilter = NULL;
    }

    ++smaIdx;
  }

  if (smaIdx <= 0) goto _err;
  pSW->number = smaIdx;

  metaReaderClear(&mr);
  taosArrayDestroy(pSmaIds);
  return pSW;
_err:
  metaReaderClear(&mr);
  taosArrayDestroy(pSmaIds);
  tdFreeTSmaWrapper(pSW, deepCopy);
  return NULL;
}

STSma *metaGetSmaInfoByIndex(SMeta *pMeta, int64_t indexUid) {
  STSma      *pTSma = NULL;
  SMetaReader mr = {0};
  metaReaderInit(&mr, pMeta, 0);
  if (metaGetTableEntryByUid(&mr, indexUid) < 0) {
    metaWarn("vgId:%d failed to get table entry for smaId: %" PRIi64, TD_VID(pMeta->pVnode), indexUid);
    metaReaderClear(&mr);
    return NULL;
  }
  pTSma = (STSma *)taosMemoryMalloc(sizeof(STSma));
  if (!pTSma) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    metaReaderClear(&mr);
    return NULL;
  }

  memcpy(pTSma, mr.me.smaEntry.tsma, sizeof(STSma));

  metaReaderClear(&mr);
  return pTSma;
}

SArray *metaGetSmaIdsByTable(SMeta *pMeta, tb_uid_t uid) {
  SArray     *pUids = NULL;
  SSmaIdxKey *pSmaIdxKey = NULL;

  SMSmaCursor *pCur = metaOpenSmaCursor(pMeta, uid);
  if (!pCur) {
    return NULL;
  }

  while (1) {
    tb_uid_t id = metaSmaCursorNext(pCur);
    if (id == 0) {
      break;
    }

    if (!pUids) {
      pUids = taosArrayInit(16, sizeof(tb_uid_t));
      if (!pUids) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        metaCloseSmaCursor(pCur);
        return NULL;
      }
    }

    pSmaIdxKey = (SSmaIdxKey *)pCur->pKey;

    if (taosArrayPush(pUids, &pSmaIdxKey->smaUid) < 0) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      metaCloseSmaCursor(pCur);
      taosArrayDestroy(pUids);
      return NULL;
    }
  }

  metaCloseSmaCursor(pCur);
  return pUids;
}

SArray *metaGetSmaTbUids(SMeta *pMeta) {
  SArray     *pUids = NULL;
  SSmaIdxKey *pSmaIdxKey = NULL;
  tb_uid_t    lastUid = 0;

  SMSmaCursor *pCur = metaOpenSmaCursor(pMeta, 0);
  if (!pCur) {
    return NULL;
  }

  while (1) {
    tb_uid_t uid = metaSmaCursorNext(pCur);
    if (uid == 0) {
      break;
    }

    if (lastUid == uid) {
      continue;
    }

    lastUid = uid;

    if (!pUids) {
      pUids = taosArrayInit(16, sizeof(tb_uid_t));
      if (!pUids) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        metaCloseSmaCursor(pCur);
        return NULL;
      }
    }

    if (taosArrayPush(pUids, &uid) < 0) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      metaCloseSmaCursor(pCur);
      taosArrayDestroy(pUids);
      return NULL;
    }
  }

  metaCloseSmaCursor(pCur);
  return pUids;
}

#endif

const void *metaGetTableTagVal(SMetaEntry *pEntry, int16_t cid) {
  ASSERT(pEntry->type == TSDB_CHILD_TABLE);
  return tdGetKVRowValOfCol((const SKVRow)pEntry->ctbEntry.pTags, cid);
}