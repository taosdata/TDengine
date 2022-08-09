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
  if (tdbTbGet(pMeta->pTbDb, &tbDbKey, sizeof(tbDbKey), &pReader->pBuf, &pReader->szBuf) < 0) {
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
  if (tdbTbGet(pMeta->pUidIdx, &uid, sizeof(uid), &pReader->pBuf, &pReader->szBuf) < 0) {
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
  if (tdbTbGet(pMeta->pNameIdx, name, strlen(name) + 1, &pReader->pBuf, &pReader->szBuf) < 0) {
    terrno = TSDB_CODE_PAR_TABLE_NOT_EXIST;
    return -1;
  }

  uid = *(tb_uid_t *)pReader->pBuf;
  return metaGetTableEntryByUid(pReader, uid);
}

tb_uid_t metaGetTableEntryUidByName(SMeta *pMeta, const char *name) {
  void    *pData = NULL;
  int      nData = 0;
  tb_uid_t uid = 0;

  metaRLock(pMeta);

  if (tdbTbGet(pMeta->pNameIdx, name, strlen(name) + 1, &pData, &nData) == 0) {
    uid = *(tb_uid_t *)pData;
    tdbFree(pData);
  }

  metaULock(pMeta);

  return uid;
}

int metaGetTableNameByUid(void *meta, uint64_t uid, char *tbName) {
  SMetaReader mr = {0};
  metaReaderInit(&mr, (SMeta *)meta, 0);
  metaGetTableEntryByUid(&mr, uid);

  STR_TO_VARSTR(tbName, mr.me.name);
  metaReaderClear(&mr);

  return 0;
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

  tdbTbcOpen(pMeta->pUidIdx, &pTbCur->pDbc, NULL);

  tdbTbcMoveToFirst(pTbCur->pDbc);

  return pTbCur;
}

void metaCloseTbCursor(SMTbCursor *pTbCur) {
  if (pTbCur) {
    tdbFree(pTbCur->pKey);
    tdbFree(pTbCur->pVal);
    metaReaderClear(&pTbCur->mr);
    if (pTbCur->pDbc) {
      tdbTbcClose(pTbCur->pDbc);
    }
    taosMemoryFree(pTbCur);
  }
}

int metaTbCursorNext(SMTbCursor *pTbCur) {
  int    ret;
  void  *pBuf;
  STbCfg tbCfg;

  for (;;) {
    ret = tdbTbcNext(pTbCur->pDbc, &pTbCur->pKey, &pTbCur->kLen, &pTbCur->pVal, &pTbCur->vLen);
    if (ret < 0) {
      return -1;
    }

    tDecoderClear(&pTbCur->mr.coder);

    metaGetTableEntryByVersion(&pTbCur->mr, *(int64_t *)pTbCur->pVal, *(tb_uid_t *)pTbCur->pKey);
    if (pTbCur->mr.me.type == TSDB_SUPER_TABLE) {
      continue;
    }

    break;
  }

  return 0;
}

SSchemaWrapper *metaGetTableSchema(SMeta *pMeta, tb_uid_t uid, int32_t sver, bool isinline) {
  void           *pData = NULL;
  int             nData = 0;
  int64_t         version;
  SSchemaWrapper  schema = {0};
  SSchemaWrapper *pSchema = NULL;
  SDecoder        dc = {0};

  metaRLock(pMeta);
_query:
  if (tdbTbGet(pMeta->pUidIdx, &uid, sizeof(uid), &pData, &nData) < 0) {
    goto _err;
  }

  version = *(int64_t *)pData;

  tdbTbGet(pMeta->pTbDb, &(STbDbKey){.uid = uid, .version = version}, sizeof(STbDbKey), &pData, &nData);
  SMetaEntry me = {0};
  tDecoderInit(&dc, pData, nData);
  metaDecodeEntry(&dc, &me);
  if (me.type == TSDB_SUPER_TABLE) {
    if (sver == -1 || sver == me.stbEntry.schemaRow.version) {
      pSchema = tCloneSSchemaWrapper(&me.stbEntry.schemaRow);
      tDecoderClear(&dc);
      goto _exit;
    }
  } else if (me.type == TSDB_CHILD_TABLE) {
    uid = me.ctbEntry.suid;
    tDecoderClear(&dc);
    goto _query;
  } else {
    if (sver == -1 || sver == me.ntbEntry.schemaRow.version) {
      pSchema = tCloneSSchemaWrapper(&me.ntbEntry.schemaRow);
      tDecoderClear(&dc);
      goto _exit;
    }
  }
  tDecoderClear(&dc);

  // query from skm db
  if (tdbTbGet(pMeta->pSkmDb, &(SSkmDbKey){.uid = uid, .sver = sver}, sizeof(SSkmDbKey), &pData, &nData) < 0) {
    goto _err;
  }

  tDecoderInit(&dc, pData, nData);
  tDecodeSSchemaWrapperEx(&dc, &schema);
  pSchema = tCloneSSchemaWrapper(&schema);
  tDecoderClear(&dc);

_exit:
  metaULock(pMeta);
  tdbFree(pData);
  return pSchema;

_err:
  metaULock(pMeta);
  tdbFree(pData);
  return NULL;
}

int metaTtlSmaller(SMeta *pMeta, uint64_t ttl, SArray *uidList) {
  TBC *pCur;
  int  ret = tdbTbcOpen(pMeta->pTtlIdx, &pCur, NULL);
  if (ret < 0) {
    return ret;
  }

  STtlIdxKey ttlKey = {0};
  ttlKey.dtime = ttl;
  ttlKey.uid = INT64_MAX;
  int c = 0;
  tdbTbcMoveTo(pCur, &ttlKey, sizeof(ttlKey), &c);
  if (c < 0) {
    tdbTbcMoveToPrev(pCur);
  }

  void *pKey = NULL;
  int   kLen = 0;
  while (1) {
    ret = tdbTbcPrev(pCur, &pKey, &kLen, NULL, NULL);
    if (ret < 0) {
      break;
    }
    ttlKey = *(STtlIdxKey *)pKey;
    taosArrayPush(uidList, &ttlKey.uid);
  }
  tdbTbcClose(pCur);

  tdbFree(pKey);

  return 0;
}

struct SMCtbCursor {
  SMeta   *pMeta;
  TBC     *pCur;
  tb_uid_t suid;
  void    *pKey;
  void    *pVal;
  int      kLen;
  int      vLen;
};

SMCtbCursor *metaOpenCtbCursor(SMeta *pMeta, tb_uid_t uid) {
  SMCtbCursor *pCtbCur = NULL;
  SCtbIdxKey   ctbIdxKey;
  int          ret = 0;
  int          c = 0;

  pCtbCur = (SMCtbCursor *)taosMemoryCalloc(1, sizeof(*pCtbCur));
  if (pCtbCur == NULL) {
    return NULL;
  }

  pCtbCur->pMeta = pMeta;
  pCtbCur->suid = uid;
  metaRLock(pMeta);

  ret = tdbTbcOpen(pMeta->pCtbIdx, &pCtbCur->pCur, NULL);
  if (ret < 0) {
    metaULock(pMeta);
    taosMemoryFree(pCtbCur);
    return NULL;
  }

  // move to the suid
  ctbIdxKey.suid = uid;
  ctbIdxKey.uid = INT64_MIN;
  tdbTbcMoveTo(pCtbCur->pCur, &ctbIdxKey, sizeof(ctbIdxKey), &c);
  if (c > 0) {
    tdbTbcMoveToNext(pCtbCur->pCur);
  }

  return pCtbCur;
}

void metaCloseCtbCursor(SMCtbCursor *pCtbCur) {
  if (pCtbCur) {
    if (pCtbCur->pMeta) metaULock(pCtbCur->pMeta);
    if (pCtbCur->pCur) {
      tdbTbcClose(pCtbCur->pCur);

      tdbFree(pCtbCur->pKey);
      tdbFree(pCtbCur->pVal);
    }

    taosMemoryFree(pCtbCur);
  }
}

tb_uid_t metaCtbCursorNext(SMCtbCursor *pCtbCur) {
  int         ret;
  SCtbIdxKey *pCtbIdxKey;

  ret = tdbTbcNext(pCtbCur->pCur, &pCtbCur->pKey, &pCtbCur->kLen, &pCtbCur->pVal, &pCtbCur->vLen);
  if (ret < 0) {
    return 0;
  }

  pCtbIdxKey = pCtbCur->pKey;
  if (pCtbIdxKey->suid > pCtbCur->suid) {
    return 0;
  }

  return pCtbIdxKey->uid;
}

struct SMStbCursor {
  SMeta   *pMeta;
  TBC     *pCur;
  tb_uid_t suid;
  void    *pKey;
  void    *pVal;
  int      kLen;
  int      vLen;
};

SMStbCursor *metaOpenStbCursor(SMeta *pMeta, tb_uid_t suid) {
  SMStbCursor *pStbCur = NULL;
  int          ret = 0;
  int          c = 0;

  pStbCur = (SMStbCursor *)taosMemoryCalloc(1, sizeof(*pStbCur));
  if (pStbCur == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pStbCur->pMeta = pMeta;
  pStbCur->suid = suid;
  metaRLock(pMeta);

  ret = tdbTbcOpen(pMeta->pSuidIdx, &pStbCur->pCur, NULL);
  if (ret < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    metaULock(pMeta);
    taosMemoryFree(pStbCur);
    return NULL;
  }

  // move to the suid
  tdbTbcMoveTo(pStbCur->pCur, &suid, sizeof(suid), &c);
  if (c > 0) {
    tdbTbcMoveToNext(pStbCur->pCur);
  }

  return pStbCur;
}

void metaCloseStbCursor(SMStbCursor *pStbCur) {
  if (pStbCur) {
    if (pStbCur->pMeta) metaULock(pStbCur->pMeta);
    if (pStbCur->pCur) {
      tdbTbcClose(pStbCur->pCur);

      tdbFree(pStbCur->pKey);
      tdbFree(pStbCur->pVal);
    }

    taosMemoryFree(pStbCur);
  }
}

tb_uid_t metaStbCursorNext(SMStbCursor *pStbCur) {
  int ret;

  ret = tdbTbcNext(pStbCur->pCur, &pStbCur->pKey, &pStbCur->kLen, &pStbCur->pVal, &pStbCur->vLen);
  if (ret < 0) {
    return 0;
  }
  return *(tb_uid_t *)pStbCur->pKey;
}

STSchema *metaGetTbTSchema(SMeta *pMeta, tb_uid_t uid, int32_t sver) {
  // SMetaReader     mr = {0};
  STSchema       *pTSchema = NULL;
  SSchemaWrapper *pSW = NULL;
  STSchemaBuilder sb = {0};
  SSchema        *pSchema;

  pSW = metaGetTableSchema(pMeta, uid, sver, 0);
  if (!pSW) return NULL;

  tdInitTSchemaBuilder(&sb, pSW->version);
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

int32_t metaGetTbTSchemaEx(SMeta *pMeta, tb_uid_t suid, tb_uid_t uid, int32_t sver, STSchema **ppTSchema) {
  int32_t   code = 0;
  STSchema *pTSchema = NULL;
  SSkmDbKey skmDbKey = {.uid = suid ? suid : uid, .sver = sver};
  void     *pData = NULL;
  int       nData = 0;

  // query
  metaRLock(pMeta);
  if (tdbTbGet(pMeta->pSkmDb, &skmDbKey, sizeof(skmDbKey), &pData, &nData) < 0) {
    code = TSDB_CODE_NOT_FOUND;
    metaULock(pMeta);
    goto _err;
  }
  metaULock(pMeta);

  // decode
  SDecoder        dc = {0};
  SSchemaWrapper  schema;
  SSchemaWrapper *pSchemaWrapper = &schema;

  tDecoderInit(&dc, pData, nData);
  tDecodeSSchemaWrapper(&dc, pSchemaWrapper);
  tDecoderClear(&dc);
  tdbFree(pData);

  // convert
  STSchemaBuilder sb = {0};

  tdInitTSchemaBuilder(&sb, pSchemaWrapper->version);
  for (int i = 0; i < pSchemaWrapper->nCols; i++) {
    SSchema *pSchema = pSchemaWrapper->pSchema + i;
    tdAddColToSchema(&sb, pSchema->type, pSchema->flags, pSchema->colId, pSchema->bytes);
  }
  pTSchema = tdGetSchemaFromBuilder(&sb);
  tdDestroyTSchemaBuilder(&sb);

  *ppTSchema = pTSchema;
  taosMemoryFree(pSchemaWrapper->pSchema);
  return code;

_err:
  *ppTSchema = NULL;
  return code;
}

// N.B. Called by statusReq per second
int64_t metaGetTbNum(SMeta *pMeta) {
  // num of child tables (excluding normal tables , stables and others)

  /* int64_t num = 0; */
  /* vnodeGetAllCtbNum(pMeta->pVnode, &num); */

  return pMeta->pVnode->config.vndStats.numOfCTables + pMeta->pVnode->config.vndStats.numOfNTables;
}

// N.B. Called by statusReq per second
int64_t metaGetTimeSeriesNum(SMeta *pMeta) {
  // sum of (number of columns of stable -  1) * number of ctables (excluding timestamp column)
  int64_t num = 0;
  vnodeGetTimeSeriesNum(pMeta->pVnode, &num);
  pMeta->pVnode->config.vndStats.numOfTimeSeries = num;

  return pMeta->pVnode->config.vndStats.numOfTimeSeries;
}

typedef struct {
  SMeta   *pMeta;
  TBC     *pCur;
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

  ret = tdbTbcOpen(pMeta->pSmaIdx, &pSmaCur->pCur, NULL);
  if (ret < 0) {
    metaULock(pMeta);
    taosMemoryFree(pSmaCur);
    return NULL;
  }

  // move to the suid
  smaIdxKey.uid = uid;
  smaIdxKey.smaUid = INT64_MIN;
  tdbTbcMoveTo(pSmaCur->pCur, &smaIdxKey, sizeof(smaIdxKey), &c);
  if (c > 0) {
    tdbTbcMoveToNext(pSmaCur->pCur);
  }

  return pSmaCur;
}

void metaCloseSmaCursor(SMSmaCursor *pSmaCur) {
  if (pSmaCur) {
    if (pSmaCur->pMeta) metaULock(pSmaCur->pMeta);
    if (pSmaCur->pCur) {
      tdbTbcClose(pSmaCur->pCur);

      tdbFree(pSmaCur->pKey);
      tdbFree(pSmaCur->pVal);
    }

    taosMemoryFree(pSmaCur);
  }
}

tb_uid_t metaSmaCursorNext(SMSmaCursor *pSmaCur) {
  int         ret;
  SSmaIdxKey *pSmaIdxKey;

  ret = tdbTbcNext(pSmaCur->pCur, &pSmaCur->pKey, &pSmaCur->kLen, &pSmaCur->pVal, &pSmaCur->vLen);
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
      tDecoderClear(&mr.coder);
      metaWarn("vgId:%d, no entry for tbId:%" PRIi64 ", smaId:%" PRIi64, TD_VID(pMeta->pVnode), uid, smaId);
      continue;
    }
    tDecoderClear(&mr.coder);
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
  tFreeTSmaWrapper(pSW, deepCopy);
  return NULL;
}

STSma *metaGetSmaInfoByIndex(SMeta *pMeta, int64_t indexUid) {
  STSma      *pTSma = NULL;
  SMetaReader mr = {0};
  metaReaderInit(&mr, pMeta, 0);
  if (metaGetTableEntryByUid(&mr, indexUid) < 0) {
    metaWarn("vgId:%d, failed to get table entry for smaId:%" PRIi64, TD_VID(pMeta->pVnode), indexUid);
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

const void *metaGetTableTagVal(SMetaEntry *pEntry, int16_t type, STagVal *val) {
  ASSERT(pEntry->type == TSDB_CHILD_TABLE);
  STag *tag = (STag *)pEntry->ctbEntry.pTags;
  if (type == TSDB_DATA_TYPE_JSON) {
    return tag;
  }
  bool find = tTagGet(tag, val);

  if (!find) {
    return NULL;
  }
  return val;
}

typedef struct {
  SMeta   *pMeta;
  TBC     *pCur;
  tb_uid_t suid;
  int16_t  cid;
  int16_t  type;
  void    *pKey;
  void    *pVal;
  int32_t  kLen;
  int32_t  vLen;
} SIdxCursor;

int32_t metaFilterTableIds(SMeta *pMeta, SMetaFltParam *param, SArray *pUids) {
  int32_t ret = 0;
  char   *buf = NULL;

  STagIdxKey *pKey = NULL;
  int32_t     nKey = 0;

  SIdxCursor *pCursor = NULL;
  pCursor = (SIdxCursor *)taosMemoryCalloc(1, sizeof(SIdxCursor));
  pCursor->pMeta = pMeta;
  pCursor->suid = param->suid;
  pCursor->cid = param->cid;
  pCursor->type = param->type;

  metaRLock(pMeta);
  ret = tdbTbcOpen(pMeta->pTagIdx, &pCursor->pCur, NULL);
  if (ret < 0) {
    goto END;
  }

  int32_t maxSize = 0;
  int32_t nTagData = 0;
  void   *tagData = NULL;

  if (param->val == NULL) {
    metaError("vgId:%d, failed to filter NULL data", TD_VID(pMeta->pVnode));
    return -1;
  } else {
    if (IS_VAR_DATA_TYPE(param->type)) {
      tagData = varDataVal(param->val);
      nTagData = varDataLen(param->val);

      if (param->type == TSDB_DATA_TYPE_NCHAR) {
        maxSize = 4 * nTagData + 1;
        buf = taosMemoryCalloc(1, maxSize);
        if (false == taosMbsToUcs4(tagData, nTagData, (TdUcs4 *)buf, maxSize, &maxSize)) {
          goto END;
        }

        tagData = buf;
        nTagData = maxSize;
      }
    } else {
      tagData = param->val;
      nTagData = tDataTypes[param->type].bytes;
    }
  }
  ret = metaCreateTagIdxKey(pCursor->suid, pCursor->cid, tagData, nTagData, pCursor->type,
                            param->reverse ? INT64_MAX : INT64_MIN, &pKey, &nKey);

  if (ret != 0) {
    goto END;
  }
  int cmp = 0;
  if (tdbTbcMoveTo(pCursor->pCur, pKey, nKey, &cmp) < 0) {
    goto END;
  }

  bool    first = true;
  int32_t valid = 0;
  while (1) {
    void   *entryKey = NULL, *entryVal = NULL;
    int32_t nEntryKey, nEntryVal;

    valid = tdbTbcGet(pCursor->pCur, (const void **)&entryKey, &nEntryKey, (const void **)&entryVal, &nEntryVal);
    if (valid < 0) {
      break;
    }
    STagIdxKey *p = entryKey;
    if (p->type != pCursor->type) {
      if (first) {
        valid = param->reverse ? tdbTbcMoveToPrev(pCursor->pCur) : tdbTbcMoveToNext(pCursor->pCur);
        if (valid < 0) break;
        continue;
      } else {
        break;
      }
    }
    first = false;
    if (p != NULL) {
      int32_t cmp = (*param->filterFunc)(p->data, pKey->data, pKey->type);
      if (cmp == 0) {
        // match
        tb_uid_t tuid = 0;
        if (IS_VAR_DATA_TYPE(pKey->type)) {
          tuid = *(tb_uid_t *)(p->data + varDataTLen(p->data));
        } else {
          tuid = *(tb_uid_t *)(p->data + tDataTypes[pCursor->type].bytes);
        }
        taosArrayPush(pUids, &tuid);
      } else if (cmp == 1) {
        // not match but should continue to iter
      } else {
        // not match and no more result
        break;
      }
    }
    valid = param->reverse ? tdbTbcMoveToPrev(pCursor->pCur) : tdbTbcMoveToNext(pCursor->pCur);
    if (valid < 0) {
      break;
    }
  }

END:
  if (pCursor->pMeta) metaULock(pCursor->pMeta);
  if (pCursor->pCur) tdbTbcClose(pCursor->pCur);
  taosMemoryFree(buf);
  taosMemoryFree(pKey);

  taosMemoryFree(pCursor);

  return ret;
}
