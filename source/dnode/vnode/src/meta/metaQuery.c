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
  if (!(flags & META_READER_NOLOCK)) {
    metaRLock(pMeta);
  }
}

void metaReaderReleaseLock(SMetaReader *pReader) {
  if (pReader->pMeta && !(pReader->flags & META_READER_NOLOCK)) {
    metaULock(pReader->pMeta);
    pReader->flags |= META_READER_NOLOCK;
  }
}

void metaReaderClear(SMetaReader *pReader) {
  if (pReader->pMeta && !(pReader->flags & META_READER_NOLOCK)) {
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

// int metaGetTableEntryByUidTest(void* meta, SArray *uidList) {
//
//   SArray* readerList = taosArrayInit(taosArrayGetSize(uidList), sizeof(SMetaReader));
//   SArray* uidVersion = taosArrayInit(taosArrayGetSize(uidList), sizeof(STbDbKey));
//   SMeta  *pMeta = meta;
//   int64_t version;
//   SHashObj *uHash = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
//
//   int64_t stt1 = taosGetTimestampUs();
//   for(int i = 0; i < taosArrayGetSize(uidList); i++) {
//     void* ppVal = NULL;
//     int vlen = 0;
//     uint64_t *  uid = taosArrayGet(uidList, i);
//     // query uid.idx
//     if (tdbTbGet(pMeta->pUidIdx, uid, sizeof(*uid), &ppVal, &vlen) < 0) {
//       continue;
//     }
//     version = *(int64_t *)ppVal;
//
//     STbDbKey tbDbKey = {.version = version, .uid = *uid};
//     taosArrayPush(uidVersion, &tbDbKey);
//     taosHashPut(uHash, uid, sizeof(int64_t), ppVal, sizeof(int64_t));
//   }
//   int64_t stt2 = taosGetTimestampUs();
//   qDebug("metaGetTableEntryByUidTest1 rows:%d, cost:%ld us", taosArrayGetSize(uidList), stt2-stt1);
//
//   TBC        *pCur = NULL;
//   tdbTbcOpen(pMeta->pTbDb, &pCur, NULL);
//   tdbTbcMoveToFirst(pCur);
//   void *pKey = NULL;
//   int   kLen = 0;
//
//   while(1){
//     SMetaReader pReader = {0};
//     int32_t ret = tdbTbcNext(pCur, &pKey, &kLen, &pReader.pBuf, &pReader.szBuf);
//     if (ret < 0) break;
//     STbDbKey *tmp = (STbDbKey*)pKey;
//     int64_t *ver = (int64_t*)taosHashGet(uHash, &tmp->uid, sizeof(int64_t));
//     if(ver == NULL || *ver != tmp->version) continue;
//     taosArrayPush(readerList, &pReader);
//   }
//   tdbTbcClose(pCur);
//
//   taosArrayClear(readerList);
//   int64_t stt3 = taosGetTimestampUs();
//   qDebug("metaGetTableEntryByUidTest2 rows:%d, cost:%ld us", taosArrayGetSize(uidList), stt3-stt2);
//   for(int i = 0; i < taosArrayGetSize(uidVersion); i++) {
//     SMetaReader pReader = {0};
//
//     STbDbKey *tbDbKey = taosArrayGet(uidVersion, i);
//     // query table.db
//     if (tdbTbGet(pMeta->pTbDb, tbDbKey, sizeof(STbDbKey), &pReader.pBuf, &pReader.szBuf) < 0) {
//       continue;
//     }
//     taosArrayPush(readerList, &pReader);
//   }
//   int64_t stt4 = taosGetTimestampUs();
//   qDebug("metaGetTableEntryByUidTest3 rows:%d, cost:%ld us", taosArrayGetSize(uidList), stt4-stt3);
//
//   for(int i = 0; i < taosArrayGetSize(readerList); i++){
//     SMetaReader* pReader  = taosArrayGet(readerList, i);
//     metaReaderInit(pReader, meta, 0);
//     // decode the entry
//     tDecoderInit(&pReader->coder, pReader->pBuf, pReader->szBuf);
//
//     if (metaDecodeEntry(&pReader->coder, &pReader->me) < 0) {
//     }
//     metaReaderClear(pReader);
//   }
//   int64_t stt5 = taosGetTimestampUs();
//   qDebug("metaGetTableEntryByUidTest4 rows:%d, cost:%ld us", taosArrayGetSize(readerList), stt5-stt4);
//   return 0;
// }

bool metaIsTableExist(SMeta *pMeta, tb_uid_t uid) {
  // query uid.idx
  metaRLock(pMeta);

  if (tdbTbGet(pMeta->pUidIdx, &uid, sizeof(uid), NULL, NULL) < 0) {
    metaULock(pMeta);

    return false;
  }

  metaULock(pMeta);

  return true;
}

int metaGetTableEntryByUid(SMetaReader *pReader, tb_uid_t uid) {
  SMeta *pMeta = pReader->pMeta;
  int64_t version1;

  // query uid.idx
  if (tdbTbGet(pMeta->pUidIdx, &uid, sizeof(uid), &pReader->pBuf, &pReader->szBuf) < 0) {
    terrno = TSDB_CODE_PAR_TABLE_NOT_EXIST;
    return -1;
  }

  version1 = ((SUidIdxVal *)pReader->pBuf)[0].version;
  return metaGetTableEntryByVersion(pReader, version1, uid);
}

int metaGetTableEntryByUidCache(SMetaReader *pReader, tb_uid_t uid) {
  SMeta *pMeta = pReader->pMeta;

  SMetaInfo info;
  if (metaGetInfo(pMeta, uid, &info, pReader) == TSDB_CODE_NOT_FOUND) {
    terrno = TSDB_CODE_PAR_TABLE_NOT_EXIST;
    return -1;
  }

  return metaGetTableEntryByVersion(pReader, info.version, uid);
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
  int         code = 0;
  SMetaReader mr = {0};
  metaReaderInit(&mr, (SMeta *)meta, 0);
  code = metaGetTableEntryByUid(&mr, uid);
  if (code < 0) {
    metaReaderClear(&mr);
    return -1;
  }

  STR_TO_VARSTR(tbName, mr.me.name);
  metaReaderClear(&mr);

  return 0;
}

int metaGetTableSzNameByUid(void *meta, uint64_t uid, char *tbName) {
  int         code = 0;
  SMetaReader mr = {0};
  metaReaderInit(&mr, (SMeta *)meta, 0);
  code = metaGetTableEntryByUid(&mr, uid);
  if (code < 0) {
    metaReaderClear(&mr);
    return -1;
  }
  strncpy(tbName, mr.me.name, TSDB_TABLE_NAME_LEN);
  metaReaderClear(&mr);

  return 0;
}


int metaGetTableUidByName(void *meta, char *tbName, uint64_t *uid) {
  int         code = 0;
  SMetaReader mr = {0};
  metaReaderInit(&mr, (SMeta *)meta, 0);

  SMetaReader *pReader = &mr;

  // query name.idx
  if (tdbTbGet(pReader->pMeta->pNameIdx, tbName, strlen(tbName) + 1, &pReader->pBuf, &pReader->szBuf) < 0) {
    terrno = TSDB_CODE_PAR_TABLE_NOT_EXIST;
    metaReaderClear(&mr);
    return -1;
  }

  *uid = *(tb_uid_t *)pReader->pBuf;

  metaReaderClear(&mr);

  return 0;
}

int metaGetTableTypeByName(void *meta, char *tbName, ETableType *tbType) {
  int         code = 0;
  SMetaReader mr = {0};
  metaReaderInit(&mr, (SMeta *)meta, 0);

  code = metaGetTableEntryByName(&mr, tbName);
  if (code == 0) *tbType = mr.me.type;

  metaReaderClear(&mr);
  return code;
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

int32_t metaTbCursorNext(SMTbCursor *pTbCur) {
  int    ret;
  void  *pBuf;
  STbCfg tbCfg;

  for (;;) {
    ret = tdbTbcNext(pTbCur->pDbc, &pTbCur->pKey, &pTbCur->kLen, &pTbCur->pVal, &pTbCur->vLen);
    if (ret < 0) {
      return -1;
    }

    tDecoderClear(&pTbCur->mr.coder);

    metaGetTableEntryByVersion(&pTbCur->mr, ((SUidIdxVal *)pTbCur->pVal)[0].version, *(tb_uid_t *)pTbCur->pKey);
    if (pTbCur->mr.me.type == TSDB_SUPER_TABLE) {
      continue;
    }

    break;
  }

  return 0;
}

int32_t metaTbCursorPrev(SMTbCursor *pTbCur) {
  int    ret;
  void  *pBuf;
  STbCfg tbCfg;

  for (;;) {
    ret = tdbTbcPrev(pTbCur->pDbc, &pTbCur->pKey, &pTbCur->kLen, &pTbCur->pVal, &pTbCur->vLen);
    if (ret < 0) {
      return -1;
    }

    tDecoderClear(&pTbCur->mr.coder);

    metaGetTableEntryByVersion(&pTbCur->mr, ((SUidIdxVal *)pTbCur->pVal)[0].version, *(tb_uid_t *)pTbCur->pKey);
    if (pTbCur->mr.me.type == TSDB_SUPER_TABLE) {
      continue;
    }

    break;
  }

  return 0;
}


SSchemaWrapper *metaGetTableSchema(SMeta *pMeta, tb_uid_t uid, int32_t sver, int lock) {
  void           *pData = NULL;
  int             nData = 0;
  int64_t         version;
  SSchemaWrapper  schema = {0};
  SSchemaWrapper *pSchema = NULL;
  SDecoder        dc = {0};
  if (lock) {
    metaRLock(pMeta);
  }
_query:
  if (tdbTbGet(pMeta->pUidIdx, &uid, sizeof(uid), &pData, &nData) < 0) {
    goto _err;
  }

  version = ((SUidIdxVal *)pData)[0].version;

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
    {  // Traverse to find the previous qualified data
      TBC *pCur;
      tdbTbcOpen(pMeta->pTbDb, &pCur, NULL);
      STbDbKey key = {.version = sver, .uid = INT64_MAX};
      int      c = 0;
      tdbTbcMoveTo(pCur, &key, sizeof(key), &c);
      if (c < 0) {
        tdbTbcMoveToPrev(pCur);
      }

      void *pKey = NULL;
      void *pVal = NULL;
      int   vLen = 0, kLen = 0;
      while (1) {
        int32_t ret = tdbTbcPrev(pCur, &pKey, &kLen, &pVal, &vLen);
        if (ret < 0) break;

        STbDbKey *tmp = (STbDbKey *)pKey;
        if (tmp->uid != uid) {
          continue;
        }
        SDecoder   dcNew = {0};
        SMetaEntry meNew = {0};
        tDecoderInit(&dcNew, pVal, vLen);
        metaDecodeEntry(&dcNew, &meNew);
        pSchema = tCloneSSchemaWrapper(&meNew.stbEntry.schemaRow);
        tDecoderClear(&dcNew);
        tdbTbcClose(pCur);
        tdbFree(pKey);
        tdbFree(pVal);
        goto _exit;
      }
      tdbFree(pKey);
      tdbFree(pVal);
      tdbTbcClose(pCur);
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
  tDecoderClear(&dc);
  if (lock) {
    metaULock(pMeta);
  }
  tdbFree(pData);
  return pSchema;

_err:
  tDecoderClear(&dc);
  if (lock) {
    metaULock(pMeta);
  }
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
  tdbFree(pKey);
  tdbTbcClose(pCur);
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

SMCtbCursor *metaOpenCtbCursor(SMeta *pMeta, tb_uid_t uid, int lock) {
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
  if (lock) {
    metaRLock(pMeta);
  }

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

void metaCloseCtbCursor(SMCtbCursor *pCtbCur, int lock) {
  if (pCtbCur) {
    if (pCtbCur->pMeta && lock) metaULock(pCtbCur->pMeta);
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

STSchema *metaGetTbTSchema(SMeta *pMeta, tb_uid_t uid, int32_t sver, int lock) {
  // SMetaReader     mr = {0};
  STSchema       *pTSchema = NULL;
  SSchemaWrapper *pSW = NULL;
  STSchemaBuilder sb = {0};
  SSchema        *pSchema;

  pSW = metaGetTableSchema(pMeta, uid, sver, lock);
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
  int32_t code = 0;

  void     *pData = NULL;
  int       nData = 0;
  SSkmDbKey skmDbKey;
  if (sver <= 0) {
    SMetaInfo info;
    if (metaGetInfo(pMeta, suid ? suid : uid, &info, NULL) == 0) {
      sver = info.skmVer;
    } else {
      TBC *pSkmDbC = NULL;
      int  c;

      skmDbKey.uid = suid ? suid : uid;
      skmDbKey.sver = INT32_MAX;

      tdbTbcOpen(pMeta->pSkmDb, &pSkmDbC, NULL);
      metaRLock(pMeta);

      if (tdbTbcMoveTo(pSkmDbC, &skmDbKey, sizeof(skmDbKey), &c) < 0) {
        metaULock(pMeta);
        tdbTbcClose(pSkmDbC);
        code = TSDB_CODE_NOT_FOUND;
        goto _exit;
      }

      ASSERT(c);

      if (c < 0) {
        tdbTbcMoveToPrev(pSkmDbC);
      }

      const void *pKey = NULL;
      int32_t     nKey = 0;
      tdbTbcGet(pSkmDbC, &pKey, &nKey, NULL, NULL);

      if (((SSkmDbKey *)pKey)->uid != skmDbKey.uid) {
        metaULock(pMeta);
        tdbTbcClose(pSkmDbC);
        code = TSDB_CODE_NOT_FOUND;
        goto _exit;
      }

      sver = ((SSkmDbKey *)pKey)->sver;

      metaULock(pMeta);
      tdbTbcClose(pSkmDbC);
    }
  }

  ASSERT(sver > 0);

  skmDbKey.uid = suid ? suid : uid;
  skmDbKey.sver = sver;
  metaRLock(pMeta);
  if (tdbTbGet(pMeta->pSkmDb, &skmDbKey, sizeof(SSkmDbKey), &pData, &nData) < 0) {
    metaULock(pMeta);
    code = TSDB_CODE_NOT_FOUND;
    goto _exit;
  }
  metaULock(pMeta);

  // decode
  SDecoder        dc = {0};
  SSchemaWrapper  schema;
  SSchemaWrapper *pSchemaWrapper = &schema;

  tDecoderInit(&dc, pData, nData);
  (void)tDecodeSSchemaWrapper(&dc, pSchemaWrapper);
  tDecoderClear(&dc);
  tdbFree(pData);

  // convert
  STSchemaBuilder sb = {0};

  tdInitTSchemaBuilder(&sb, pSchemaWrapper->version);
  for (int i = 0; i < pSchemaWrapper->nCols; i++) {
    SSchema *pSchema = pSchemaWrapper->pSchema + i;
    tdAddColToSchema(&sb, pSchema->type, pSchema->flags, pSchema->colId, pSchema->bytes);
  }

  STSchema *pTSchema = tdGetSchemaFromBuilder(&sb);
  if (pTSchema == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }

  tdDestroyTSchemaBuilder(&sb);

  *ppTSchema = pTSchema;
  taosMemoryFree(pSchemaWrapper->pSchema);

_exit:
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
  if (pMeta->pVnode->config.vndStats.numOfTimeSeries <= 0 ||
      ++pMeta->pVnode->config.vndStats.itvTimeSeries % (60 * 5) == 0) {
    int64_t num = 0;
    vnodeGetTimeSeriesNum(pMeta->pVnode, &num);
    pMeta->pVnode->config.vndStats.numOfTimeSeries = num;

    pMeta->pVnode->config.vndStats.itvTimeSeries = (TD_VID(pMeta->pVnode) % 100) * 2;
  }

  return pMeta->pVnode->config.vndStats.numOfTimeSeries + pMeta->pVnode->config.vndStats.numOfNTimeSeries;
}

int64_t metaGetNtbNum(SMeta *pMeta) {
  return pMeta->pVnode->config.vndStats.numOfNTables;
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

    if (!taosArrayPush(pUids, &pSmaIdxKey->smaUid)) {
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

    if (!taosArrayPush(pUids, &uid)) {
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

const void *metaGetTableTagVal(void *pTag, int16_t type, STagVal *val) {
  STag *tag = (STag *)pTag;
  if (type == TSDB_DATA_TYPE_JSON) {
    return tag;
  }
  bool find = tTagGet(tag, val);

  if (!find) {
    return NULL;
  }

#ifdef TAG_FILTER_DEBUG
  if (IS_VAR_DATA_TYPE(val->type)) {
    char *buf = taosMemoryCalloc(val->nData + 1, 1);
    memcpy(buf, val->pData, val->nData);
    metaDebug("metaTag table val varchar index:%d cid:%d type:%d value:%s", 1, val->cid, val->type, buf);
    taosMemoryFree(buf);
  } else {
    double dval = 0;
    GET_TYPED_DATA(dval, double, val->type, &val->i64);
    metaDebug("metaTag table val number index:%d cid:%d type:%d value:%f", 1, val->cid, val->type, dval);
  }

  SArray *pTagVals = NULL;
  tTagToValArray((STag *)pTag, &pTagVals);
  for (int i = 0; i < taosArrayGetSize(pTagVals); i++) {
    STagVal *pTagVal = (STagVal *)taosArrayGet(pTagVals, i);

    if (IS_VAR_DATA_TYPE(pTagVal->type)) {
      char *buf = taosMemoryCalloc(pTagVal->nData + 1, 1);
      memcpy(buf, pTagVal->pData, pTagVal->nData);
      metaDebug("metaTag table varchar index:%d cid:%d type:%d value:%s", i, pTagVal->cid, pTagVal->type, buf);
      taosMemoryFree(buf);
    } else {
      double dval = 0;
      GET_TYPED_DATA(dval, double, pTagVal->type, &pTagVal->i64);
      metaDebug("metaTag table number index:%d cid:%d type:%d value:%f", i, pTagVal->cid, pTagVal->type, dval);
    }
  }
#endif

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

int32_t metaFilterCreateTime(SMeta *pMeta, SMetaFltParam *param, SArray *pUids) {
  int32_t ret = 0;

  SIdxCursor *pCursor = NULL;
  pCursor = (SIdxCursor *)taosMemoryCalloc(1, sizeof(SIdxCursor));
  pCursor->pMeta = pMeta;
  pCursor->suid = param->suid;
  pCursor->cid = param->cid;
  pCursor->type = param->type;

  metaRLock(pMeta);
  ret = tdbTbcOpen(pMeta->pCtimeIdx, &pCursor->pCur, NULL);
  if (ret != 0) {
    goto END;
  }
  int64_t uidLimit = param->reverse ? INT64_MAX : 0;

  SCtimeIdxKey  ctimeKey = {.ctime = *(int64_t *)(param->val), .uid = uidLimit};
  SCtimeIdxKey *pCtimeKey = &ctimeKey;

  int cmp = 0;
  if (tdbTbcMoveTo(pCursor->pCur, &ctimeKey, sizeof(ctimeKey), &cmp) < 0) {
    goto END;
  }

  int32_t valid = 0;
  while (1) {
    void   *entryKey = NULL;
    int32_t nEntryKey = -1;
    valid = tdbTbcGet(pCursor->pCur, (const void **)&entryKey, &nEntryKey, NULL, NULL);
    if (valid < 0) break;

    SCtimeIdxKey *p = entryKey;

    int32_t cmp = (*param->filterFunc)((void *)&p->ctime, (void *)&pCtimeKey->ctime, param->type);
    if (cmp == 0) taosArrayPush(pUids, &p->uid);

    if (param->reverse == false) {
      if (cmp == -1) break;
    } else if (param->reverse) {
      if (cmp == 1) break;
    }

    valid = param->reverse ? tdbTbcMoveToPrev(pCursor->pCur) : tdbTbcMoveToNext(pCursor->pCur);
    if (valid < 0) break;
  }

END:
  if (pCursor->pMeta) metaULock(pCursor->pMeta);
  if (pCursor->pCur) tdbTbcClose(pCursor->pCur);
  taosMemoryFree(pCursor);
  return ret;
}

int32_t metaFilterTableName(SMeta *pMeta, SMetaFltParam *param, SArray *pUids) {
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

  char *pName = param->val;

  metaRLock(pMeta);
  ret = tdbTbcOpen(pMeta->pNameIdx, &pCursor->pCur, NULL);
  if (ret != 0) {
    goto END;
  }

  int cmp = 0;
  if (tdbTbcMoveTo(pCursor->pCur, pName, strlen(pName) + 1, &cmp) < 0) {
    goto END;
  }
  bool    first = true;
  int32_t valid = 0;
  while (1) {
    void   *pEntryKey = NULL, *pEntryVal = NULL;
    int32_t nEntryKey = -1, nEntryVal = 0;
    valid = tdbTbcGet(pCursor->pCur, (const void **)pEntryKey, &nEntryKey, (const void **)&pEntryVal, &nEntryVal);
    if (valid < 0) break;

    char *pTableKey = (char *)pEntryKey;
    cmp = (*param->filterFunc)(pTableKey, pName, pCursor->type);
    if (cmp == 0) {
      tb_uid_t tuid = *(tb_uid_t *)pEntryVal;
      taosArrayPush(pUids, &tuid);
    } else if (cmp == 1) {
      // next
    } else {
      break;
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
int32_t metaFilterTtl(SMeta *pMeta, SMetaFltParam *param, SArray *pUids) {
  int32_t ret = 0;
  char   *buf = NULL;

  STtlIdxKey *pKey = NULL;
  int32_t     nKey = 0;

  SIdxCursor *pCursor = NULL;
  pCursor = (SIdxCursor *)taosMemoryCalloc(1, sizeof(SIdxCursor));
  pCursor->pMeta = pMeta;
  pCursor->suid = param->suid;
  pCursor->cid = param->cid;
  pCursor->type = param->type;

  metaRLock(pMeta);
  ret = tdbTbcOpen(pMeta->pTtlIdx, &pCursor->pCur, NULL);

END:
  if (pCursor->pMeta) metaULock(pCursor->pMeta);
  if (pCursor->pCur) tdbTbcClose(pCursor->pCur);
  taosMemoryFree(buf);
  taosMemoryFree(pKey);

  taosMemoryFree(pCursor);

  return ret;
  // impl later
  return 0;
}
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
  ret = tdbTbcOpen(pMeta->pCtimeIdx, &pCursor->pCur, NULL);
  if (ret < 0) {
    goto END;
  }

  int32_t maxSize = 0;
  int32_t nTagData = 0;
  void   *tagData = NULL;

  if (param->val == NULL) {
    metaError("vgId:%d, failed to filter NULL data", TD_VID(pMeta->pVnode));
    ret = -1;
    goto END;
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
      tdbFree(entryVal);
      break;
    }
    STagIdxKey *p = entryKey;
    if (p == NULL) break;
    if (p->type != pCursor->type) {
      if (first) {
        valid = param->reverse ? tdbTbcMoveToPrev(pCursor->pCur) : tdbTbcMoveToNext(pCursor->pCur);
        if (valid < 0) break;
        continue;
      } else {
        break;
      }
    }
    if (p->suid != pKey->suid) {
      break;
    }
    first = false;
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

static int32_t metaGetTableTagByUid(SMeta *pMeta, int64_t suid, int64_t uid, void **tag, int32_t *len, bool lock) {
  int ret = 0;
  if (lock) {
    metaRLock(pMeta);
  }

  SCtbIdxKey ctbIdxKey = {.suid = suid, .uid = uid};
  ret = tdbTbGet(pMeta->pCtbIdx, &ctbIdxKey, sizeof(SCtbIdxKey), tag, len);
  if (lock) {
    metaULock(pMeta);
  }

  return ret;
}
int32_t metaGetTableTagsByUids(SMeta *pMeta, int64_t suid, SArray *uidList, SHashObj *tags) {
  const int32_t LIMIT = 128;

  int32_t isLock = false;
  int32_t sz = uidList ? taosArrayGetSize(uidList) : 0;
  for (int i = 0; i < sz; i++) {
    tb_uid_t *id = taosArrayGet(uidList, i);

    if (i % LIMIT == 0) {
      if (isLock) metaULock(pMeta);

      metaRLock(pMeta);
      isLock = true;
    }

    if (taosHashGet(tags, id, sizeof(tb_uid_t)) == NULL) {
      void   *val = NULL;
      int32_t len = 0;
      if (metaGetTableTagByUid(pMeta, suid, *id, &val, &len, false) == 0) {
        taosHashPut(tags, id, sizeof(tb_uid_t), val, len);
        tdbFree(val);
      } else {
        metaError("vgId:%d, failed to table IDs, suid: %" PRId64 ", uid: %" PRId64 "", TD_VID(pMeta->pVnode), suid,
                  *id);
      }
    }
  }
  if (isLock) metaULock(pMeta);

  return 0;
}

int32_t metaGetTableTags(SMeta *pMeta, uint64_t suid, SArray *uidList, SHashObj *tags) {
  SMCtbCursor *pCur = metaOpenCtbCursor(pMeta, suid, 1);

  SHashObj *uHash = NULL;
  size_t    len = taosArrayGetSize(uidList);  // len > 0 means there already have uids
  if (len > 0) {
    uHash = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
    for (int i = 0; i < len; i++) {
      int64_t *uid = taosArrayGet(uidList, i);
      taosHashPut(uHash, uid, sizeof(int64_t), &i, sizeof(i));
    }
  }
  while (1) {
    tb_uid_t id = metaCtbCursorNext(pCur);
    if (id == 0) {
      break;
    }

    if (len > 0 && taosHashGet(uHash, &id, sizeof(int64_t)) == NULL) {
      continue;
    } else if (len == 0) {
      taosArrayPush(uidList, &id);
    }

    taosHashPut(tags, &id, sizeof(int64_t), pCur->pVal, pCur->vLen);
  }

  taosHashCleanup(uHash);
  metaCloseCtbCursor(pCur, 1);
  return TSDB_CODE_SUCCESS;
}

int32_t metaCacheGet(SMeta *pMeta, int64_t uid, SMetaInfo *pInfo);

int32_t metaGetInfo(SMeta *pMeta, int64_t uid, SMetaInfo *pInfo, SMetaReader *pReader) {
  int32_t code = 0;
  void   *pData = NULL;
  int     nData = 0;
  int     lock = 0;

  metaRLock(pMeta);

  // search cache
  if (metaCacheGet(pMeta, uid, pInfo) == 0) {
    metaULock(pMeta);
    goto _exit;
  }

  // search TDB
  if (tdbTbGet(pMeta->pUidIdx, &uid, sizeof(uid), &pData, &nData) < 0) {
    // not found
    metaULock(pMeta);
    code = TSDB_CODE_NOT_FOUND;
    goto _exit;
  }

  metaULock(pMeta);

  pInfo->uid = uid;
  pInfo->suid = ((SUidIdxVal *)pData)->suid;
  pInfo->version = ((SUidIdxVal *)pData)->version;
  pInfo->skmVer = ((SUidIdxVal *)pData)->skmVer;

  if (pReader != NULL) {
    lock = !(pReader->flags & META_READER_NOLOCK);
    if (lock) {
      metaULock(pReader->pMeta);
      // metaReaderReleaseLock(pReader);
    }
  }
  // upsert the cache
  metaWLock(pMeta);
  metaCacheUpsert(pMeta, pInfo);
  metaULock(pMeta);

  if (lock) {
    metaRLock(pReader->pMeta);
  }

_exit:
  tdbFree(pData);
  return code;
}

int32_t metaGetStbStats(SMeta *pMeta, int64_t uid, SMetaStbStats *pInfo) {
  int32_t code = 0;

  metaRLock(pMeta);

  // fast path: search cache
  if (metaStatsCacheGet(pMeta, uid, pInfo) == TSDB_CODE_SUCCESS) {
    metaULock(pMeta);
    goto _exit;
  }

  // slow path: search TDB
  int64_t ctbNum = 0;
  vnodeGetCtbNum(pMeta->pVnode, uid, &ctbNum);

  metaULock(pMeta);

  pInfo->uid = uid;
  pInfo->ctbNum = ctbNum;

  // upsert the cache
  metaWLock(pMeta);
  metaStatsCacheUpsert(pMeta, pInfo);
  metaULock(pMeta);

_exit:
  return code;
}

void metaUpdateStbStats(SMeta *pMeta, int64_t uid, int64_t delta) {
  SMetaStbStats stats = {0};

  if (metaStatsCacheGet(pMeta, uid, &stats) == TSDB_CODE_SUCCESS) {
    stats.ctbNum += delta;

    metaStatsCacheUpsert(pMeta, &stats);
  }
}
