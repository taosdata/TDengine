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

#ifndef META_REFACT

typedef struct SPoolMem {
  int64_t          size;
  struct SPoolMem *prev;
  struct SPoolMem *next;
} SPoolMem;

#define META_TDB_SMA_TEST

static SPoolMem *openPool();
static void      clearPool(SPoolMem *pPool);
static void      closePool(SPoolMem *pPool);
static void     *poolMalloc(void *arg, size_t size);
static void      poolFree(void *arg, void *ptr);

struct SMetaDB {
  TXN       txn;
  TENV     *pEnv;
  TDB      *pTbDB;
  TDB      *pSchemaDB;
  TDB      *pNameIdx;
  TDB      *pStbIdx;
  TDB      *pNtbIdx;
  TDB      *pCtbIdx;
  SPoolMem *pPool;
#ifdef META_TDB_SMA_TEST
  TDB *pSmaDB;
  TDB *pSmaIdx;
#endif
};

#pragma pack(push, 1)
typedef struct {
  tb_uid_t uid;
  int32_t  sver;
} SSchemaDbKey;
#pragma pack(pop)

typedef struct {
  char    *name;
  tb_uid_t uid;
} SNameIdxKey;

typedef struct {
  tb_uid_t suid;
  tb_uid_t uid;
} SCtbIdxKey;

typedef struct {
  tb_uid_t uid;
  int64_t  smaUid;
} SSmaIdxKey;

static int   metaEncodeTbInfo(void **buf, STbCfg *pTbCfg);
static void *metaDecodeTbInfo(void *buf, STbCfg *pTbCfg);
static int   metaEncodeSchema(void **buf, SSchemaWrapper *pSW);
static void *metaDecodeSchema(void *buf, SSchemaWrapper *pSW);
static int   metaEncodeSchemaEx(void **buf, SSchemaWrapper *pSW);
static void *metaDecodeSchemaEx(void *buf, SSchemaWrapper *pSW, bool isGetEx);

static SSchemaWrapper *metaGetTableSchemaImpl(SMeta *pMeta, tb_uid_t uid, int32_t sver, bool isinline, bool isGetEx);

static inline int metaUidCmpr(const void *arg1, int len1, const void *arg2, int len2) {
  tb_uid_t uid1, uid2;

  ASSERT(len1 == sizeof(tb_uid_t));
  ASSERT(len2 == sizeof(tb_uid_t));

  uid1 = ((tb_uid_t *)arg1)[0];
  uid2 = ((tb_uid_t *)arg2)[0];

  if (uid1 < uid2) {
    return -1;
  }
  if (uid1 == uid2) {
    return 0;
  } else {
    return 1;
  }
}

static inline int metaSchemaKeyCmpr(const void *arg1, int len1, const void *arg2, int len2) {
  int           c;
  SSchemaDbKey *pKey1 = (SSchemaDbKey *)arg1;
  SSchemaDbKey *pKey2 = (SSchemaDbKey *)arg2;

  c = metaUidCmpr(arg1, sizeof(tb_uid_t), arg2, sizeof(tb_uid_t));
  if (c) return c;

  if (pKey1->sver > pKey2->sver) {
    return 1;
  } else if (pKey1->sver == pKey2->sver) {
    return 0;
  } else {
    return -1;
  }
}

static inline int metaNameIdxCmpr(const void *arg1, int len1, const void *arg2, int len2) {
  return strcmp((char *)arg1, (char *)arg2);
}

static inline int metaCtbIdxCmpr(const void *arg1, int len1, const void *arg2, int len2) {
  int         c;
  SCtbIdxKey *pKey1 = (SCtbIdxKey *)arg1;
  SCtbIdxKey *pKey2 = (SCtbIdxKey *)arg2;

  c = metaUidCmpr(arg1, sizeof(tb_uid_t), arg2, sizeof(tb_uid_t));
  if (c) return c;

  return metaUidCmpr(&pKey1->uid, sizeof(tb_uid_t), &pKey2->uid, sizeof(tb_uid_t));
}

static inline int metaSmaIdxCmpr(const void *arg1, int len1, const void *arg2, int len2) {
  int         c;
  SSmaIdxKey *pKey1 = (SSmaIdxKey *)arg1;
  SSmaIdxKey *pKey2 = (SSmaIdxKey *)arg2;

  c = metaUidCmpr(arg1, sizeof(tb_uid_t), arg2, sizeof(tb_uid_t));
  if (c) return c;

  return metaUidCmpr(&pKey1->smaUid, sizeof(int64_t), &pKey2->smaUid, sizeof(int64_t));
}

int metaOpenDB(SMeta *pMeta) {
  SMetaDB *pMetaDb;
  int      ret;

  // allocate DB handle
  pMetaDb = taosMemoryCalloc(1, sizeof(*pMetaDb));
  if (pMetaDb == NULL) {
    // TODO
    ASSERT(0);
    return -1;
  }

  // open the ENV
  ret = tdbEnvOpen(pMeta->path, 4096, 256, &(pMetaDb->pEnv));
  if (ret < 0) {
    // TODO
    ASSERT(0);
    return -1;
  }

  // open table DB
  ret = tdbDbOpen("table.db", sizeof(tb_uid_t), TDB_VARIANT_LEN, metaUidCmpr, pMetaDb->pEnv, &(pMetaDb->pTbDB));
  if (ret < 0) {
    // TODO
    ASSERT(0);
    return -1;
  }

#ifdef META_TDB_SMA_TEST
  ret = tdbDbOpen("sma.db", sizeof(int64_t), TDB_VARIANT_LEN, metaUidCmpr, pMetaDb->pEnv, &(pMetaDb->pSmaDB));
  if (ret < 0) {
    // TODO
    ASSERT(0);
    return -1;
  }
#endif

  // open schema DB
  ret = tdbDbOpen("schema.db", sizeof(SSchemaDbKey), TDB_VARIANT_LEN, metaSchemaKeyCmpr, pMetaDb->pEnv,
                  &(pMetaDb->pSchemaDB));
  if (ret < 0) {
    // TODO
    ASSERT(0);
    return -1;
  }

  ret = tdbDbOpen("name.idx", TDB_VARIANT_LEN, 0, metaNameIdxCmpr, pMetaDb->pEnv, &(pMetaDb->pNameIdx));
  if (ret < 0) {
    // TODO
    ASSERT(0);
    return -1;
  }

  ret = tdbDbOpen("stb.idx", sizeof(tb_uid_t), 0, metaUidCmpr, pMetaDb->pEnv, &(pMetaDb->pStbIdx));
  if (ret < 0) {
    // TODO
    ASSERT(0);
    return -1;
  }

  ret = tdbDbOpen("ntb.idx", sizeof(tb_uid_t), 0, metaUidCmpr, pMetaDb->pEnv, &(pMetaDb->pNtbIdx));
  if (ret < 0) {
    // TODO
    ASSERT(0);
    return -1;
  }

  ret = tdbDbOpen("ctb.idx", sizeof(SCtbIdxKey), 0, metaCtbIdxCmpr, pMetaDb->pEnv, &(pMetaDb->pCtbIdx));
  if (ret < 0) {
    // TODO
    ASSERT(0);
    return -1;
  }

#ifdef META_TDB_SMA_TEST
  ret = tdbDbOpen("sma.idx", sizeof(SSmaIdxKey), 0, metaSmaIdxCmpr, pMetaDb->pEnv, &(pMetaDb->pSmaIdx));
  if (ret < 0) {
    // TODO
    ASSERT(0);
    return -1;
  }
#endif

  pMetaDb->pPool = openPool();
  tdbTxnOpen(&pMetaDb->txn, 0, poolMalloc, poolFree, pMetaDb->pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);
  tdbBegin(pMetaDb->pEnv, NULL);

  pMeta->pDB = pMetaDb;
  return 0;
}

void metaCloseDB(SMeta *pMeta) {
  if (pMeta->pDB) {
    tdbCommit(pMeta->pDB->pEnv, &pMeta->pDB->txn);
    tdbTxnClose(&pMeta->pDB->txn);
    clearPool(pMeta->pDB->pPool);
#ifdef META_TDB_SMA_TEST
    tdbDbClose(pMeta->pDB->pSmaIdx);
#endif
    tdbDbClose(pMeta->pDB->pCtbIdx);
    tdbDbClose(pMeta->pDB->pNtbIdx);
    tdbDbClose(pMeta->pDB->pStbIdx);
    tdbDbClose(pMeta->pDB->pNameIdx);
#ifdef META_TDB_SMA_TEST
    tdbDbClose(pMeta->pDB->pSmaDB);
#endif
    tdbDbClose(pMeta->pDB->pSchemaDB);
    tdbDbClose(pMeta->pDB->pTbDB);
    taosMemoryFree(pMeta->pDB);
  }
}

int metaSaveTableToDB(SMeta *pMeta, STbCfg *pTbCfg, STbDdlH *pHandle) {
  tb_uid_t       uid;
  SMetaDB       *pMetaDb;
  void          *pKey;
  void          *pVal;
  int            kLen;
  int            vLen;
  int            ret;
  char           buf[512];
  void          *pBuf;
  SCtbIdxKey     ctbIdxKey;
  SSchemaDbKey   schemaDbKey;
  SSchemaWrapper schemaWrapper;

  pMetaDb = pMeta->pDB;

  // TODO: make this operation pre-process
  if (pTbCfg->type == META_SUPER_TABLE) {
    uid = pTbCfg->stbCfg.suid;
  } else {
    uid = metaGenerateUid(pMeta);
  }

  // check name and uid unique
  if (tdbDbGet(pMetaDb->pTbDB, &uid, sizeof(uid), NULL, NULL) == 0) {
    return -1;
  }
  if (tdbDbGet(pMetaDb->pNameIdx, pTbCfg->name, strlen(pTbCfg->name) + 1, NULL, NULL) == 0) {
    return -1;
  }

  // save to table.db
  pKey = &uid;
  kLen = sizeof(uid);
  pVal = pBuf = buf;
  metaEncodeTbInfo(&pBuf, pTbCfg);
  vLen = POINTER_DISTANCE(pBuf, buf);
  ret = tdbDbInsert(pMetaDb->pTbDB, pKey, kLen, pVal, vLen, &pMetaDb->txn);
  if (ret < 0) {
    return -1;
  }

  // save to schema.db for META_SUPER_TABLE and META_NORMAL_TABLE
  if (pTbCfg->type != META_CHILD_TABLE) {
    schemaDbKey.uid = uid;
    schemaDbKey.sver = 0;  // TODO
    pKey = &schemaDbKey;
    kLen = sizeof(schemaDbKey);

    if (pTbCfg->type == META_SUPER_TABLE) {
      schemaWrapper.nCols = pTbCfg->stbCfg.nCols;
      schemaWrapper.pSchema = pTbCfg->stbCfg.pSchema;
    } else {
      schemaWrapper.nCols = pTbCfg->ntbCfg.nCols;
      schemaWrapper.pSchema = pTbCfg->ntbCfg.pSchema;
    }
    pVal = pBuf = buf;
    metaEncodeSchemaEx(&pBuf, &schemaWrapper);
    vLen = POINTER_DISTANCE(pBuf, buf);
    ret = tdbDbInsert(pMetaDb->pSchemaDB, pKey, kLen, pVal, vLen, &pMeta->pDB->txn);
    if (ret < 0) {
      return -1;
    }
  }

  // update name.idx
  int nameLen = strlen(pTbCfg->name);
  memcpy(buf, pTbCfg->name, nameLen + 1);
  ((tb_uid_t *)(buf + nameLen + 1))[0] = uid;
  pKey = buf;
  kLen = nameLen + 1 + sizeof(uid);
  pVal = NULL;
  vLen = 0;
  ret = tdbDbInsert(pMetaDb->pNameIdx, pKey, kLen, pVal, vLen, &pMetaDb->txn);
  if (ret < 0) {
    return -1;
  }

  // update other index
  if (pTbCfg->type == META_SUPER_TABLE) {
    pKey = &uid;
    kLen = sizeof(uid);
    pVal = NULL;
    vLen = 0;
    ret = tdbDbInsert(pMetaDb->pStbIdx, pKey, kLen, pVal, vLen, &pMetaDb->txn);
    if (ret < 0) {
      return -1;
    }
  } else if (pTbCfg->type == META_CHILD_TABLE) {
    ctbIdxKey.suid = pTbCfg->ctbCfg.suid;
    ctbIdxKey.uid = uid;
    pKey = &ctbIdxKey;
    kLen = sizeof(ctbIdxKey);
    pVal = NULL;
    vLen = 0;
    ret = tdbDbInsert(pMetaDb->pCtbIdx, pKey, kLen, pVal, vLen, &pMetaDb->txn);
    if (ret < 0) {
      return -1;
    }
    // child table handle for rsma
    if (pHandle && pHandle->fp) {
      if (((*pHandle->fp)(pHandle->ahandle, &pHandle->result, &ctbIdxKey.suid, &uid)) < 0) {
        return -1;
      };
    }
  } else if (pTbCfg->type == META_NORMAL_TABLE) {
    pKey = &uid;
    kLen = sizeof(uid);
    pVal = NULL;
    vLen = 0;
    ret = tdbDbInsert(pMetaDb->pNtbIdx, pKey, kLen, pVal, vLen, &pMetaDb->txn);
    if (ret < 0) {
      return -1;
    }
  }

  if (pMeta->pDB->pPool->size > 0) {
    metaCommit(pMeta);
  }

  return 0;
}

int metaRemoveTableFromDb(SMeta *pMeta, tb_uid_t uid) {
  // TODO
  ASSERT(0);
  return 0;
}

static SSchemaWrapper *metaGetTableSchemaImpl(SMeta *pMeta, tb_uid_t uid, int32_t sver, bool isinline, bool isGetEx) {
  void           *pKey;
  void           *pVal;
  int             kLen;
  int             vLen;
  int             ret;
  SSchemaDbKey    schemaDbKey;
  SSchemaWrapper *pSchemaWrapper;
  void           *pBuf;

  // fetch
  schemaDbKey.uid = uid;
  schemaDbKey.sver = sver;
  pKey = &schemaDbKey;
  kLen = sizeof(schemaDbKey);
  pVal = NULL;
  ret = tdbDbGet(pMeta->pDB->pSchemaDB, pKey, kLen, &pVal, &vLen);
  if (ret < 0) {
    return NULL;
  }

  // decode
  pBuf = pVal;
  pSchemaWrapper = taosMemoryMalloc(sizeof(*pSchemaWrapper));
  metaDecodeSchemaEx(pBuf, pSchemaWrapper, isGetEx);

  TDB_FREE(pVal);

  return pSchemaWrapper;
}

struct SMSmaCursor {
  TDBC    *pCur;
  tb_uid_t uid;
  void    *pKey;
  void    *pVal;
  int      kLen;
  int      vLen;
};

STSmaWrapper *metaGetSmaInfoByTable(SMeta *pMeta, tb_uid_t uid) {
  // TODO
  // ASSERT(0);
  // return NULL;
#ifdef META_TDB_SMA_TEST
  STSmaWrapper *pSW = NULL;

  SMSmaCursor *pCur = metaOpenSmaCursor(pMeta, uid);
  if (pCur == NULL) {
    return NULL;
  }

  void       *pBuf = NULL;
  SSmaIdxKey *pSmaIdxKey = NULL;

  while (true) {
    // TODO: lock during iterate?
    if (tdbDbNext(pCur->pCur, &pCur->pKey, &pCur->kLen, NULL, &pCur->vLen) == 0) {
      pSmaIdxKey = pCur->pKey;
      ASSERT(pSmaIdxKey != NULL);

      void *pSmaVal = metaGetSmaInfoByIndex(pMeta, pSmaIdxKey->smaUid, false);

      if (pSmaVal == NULL) {
        tsdbWarn("no tsma exists for indexUid: %" PRIi64, pSmaIdxKey->smaUid);
        continue;
      }

      if ((pSW == NULL) && ((pSW = taosMemoryCalloc(1, sizeof(*pSW))) == NULL)) {
        TDB_FREE(pSmaVal);
        metaCloseSmaCursor(pCur);
        return NULL;
      }

      ++pSW->number;
      STSma *tptr = (STSma *)taosMemoryRealloc(pSW->tSma, pSW->number * sizeof(STSma));
      if (tptr == NULL) {
        TDB_FREE(pSmaVal);
        metaCloseSmaCursor(pCur);
        tdDestroyTSmaWrapper(pSW);
        taosMemoryFreeClear(pSW);
        return NULL;
      }
      pSW->tSma = tptr;
      pBuf = pSmaVal;
      if (tDecodeTSma(pBuf, pSW->tSma + pSW->number - 1) == NULL) {
        TDB_FREE(pSmaVal);
        metaCloseSmaCursor(pCur);
        tdDestroyTSmaWrapper(pSW);
        taosMemoryFreeClear(pSW);
        return NULL;
      }
      TDB_FREE(pSmaVal);
      continue;
    }
    break;
  }

  metaCloseSmaCursor(pCur);

  return pSW;

#endif
}

int metaRemoveSmaFromDb(SMeta *pMeta, int64_t indexUid) {
  // TODO
  ASSERT(0);
#ifndef META_TDB_SMA_TEST
  DBT key = {0};

  key.data = (void *)indexName;
  key.size = strlen(indexName);

  metaDBWLock(pMeta->pDB);
  // TODO: No guarantee of consistence.
  // Use transaction or DB->sync() for some guarantee.
  pMeta->pDB->pSmaDB->del(pMeta->pDB->pSmaDB, NULL, &key, 0);
  metaDBULock(pMeta->pDB);
#endif
  return 0;
}

int metaSaveSmaToDB(SMeta *pMeta, STSma *pSmaCfg) {
  // TODO
  // ASSERT(0);

#ifdef META_TDB_SMA_TEST
  int32_t  ret = 0;
  SMetaDB *pMetaDb = pMeta->pDB;
  void    *pBuf = NULL, *qBuf = NULL;
  void    *key = {0}, *val = {0};

  // save sma info
  int32_t len = tEncodeTSma(NULL, pSmaCfg);
  pBuf = taosMemoryCalloc(1, len);
  if (pBuf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  key = (void *)&pSmaCfg->indexUid;
  qBuf = pBuf;
  tEncodeTSma(&qBuf, pSmaCfg);
  val = pBuf;

  int32_t kLen = sizeof(pSmaCfg->indexUid);
  int32_t vLen = POINTER_DISTANCE(qBuf, pBuf);

  ret = tdbDbInsert(pMeta->pDB->pSmaDB, key, kLen, val, vLen, &pMetaDb->txn);
  if (ret < 0) {
    taosMemoryFreeClear(pBuf);
    return -1;
  }

  // add sma idx
  SSmaIdxKey smaIdxKey;
  smaIdxKey.uid = pSmaCfg->tableUid;
  smaIdxKey.smaUid = pSmaCfg->indexUid;
  key = &smaIdxKey;
  kLen = sizeof(smaIdxKey);
  val = NULL;
  vLen = 0;

  ret = tdbDbInsert(pMeta->pDB->pSmaIdx, key, kLen, val, vLen, &pMetaDb->txn);
  if (ret < 0) {
    taosMemoryFreeClear(pBuf);
    return -1;
  }

  // release
  taosMemoryFreeClear(pBuf);

  if (pMeta->pDB->pPool->size > 0) {
    metaCommit(pMeta);
  }

#endif
  return 0;
}

/**
 * @brief
 *
 * @param pMeta
 * @param uid 0 means iterate all uids.
 * @return SMSmaCursor*
 */
SMSmaCursor *metaOpenSmaCursor(SMeta *pMeta, tb_uid_t uid) {
  // TODO
  // ASSERT(0);
  // return NULL;
#ifdef META_TDB_SMA_TEST
  SMSmaCursor *pCur = NULL;
  SMetaDB     *pDB = pMeta->pDB;
  int          ret;

  pCur = (SMSmaCursor *)taosMemoryCalloc(1, sizeof(*pCur));
  if (pCur == NULL) {
    return NULL;
  }

  pCur->uid = uid;
  ret = tdbDbcOpen(pDB->pSmaIdx, &(pCur->pCur));
  if ((ret != 0) || (pCur->pCur == NULL)) {
    taosMemoryFree(pCur);
    return NULL;
  }

  if (uid != 0) {
    // TODO: move to the specific uid
  }

  return pCur;
#endif
}

/**
 * @brief
 *
 * @param pCur
 * @return int64_t smaIndexUid
 */
int64_t metaSmaCursorNext(SMSmaCursor *pCur) {
  // TODO
  // ASSERT(0);
  // return NULL;
#ifdef META_TDB_SMA_TEST
  int         ret;
  void       *pBuf;
  SSmaIdxKey *smaIdxKey;

  ret = tdbDbNext(pCur->pCur, &pCur->pKey, &pCur->kLen, &pCur->pVal, &pCur->vLen);
  if (ret < 0) {
    return 0;
  }
  smaIdxKey = pCur->pKey;
  return smaIdxKey->smaUid;
#endif
}

void metaCloseSmaCursor(SMSmaCursor *pCur) {
  // TODO
  // ASSERT(0);
#ifdef META_TDB_SMA_TEST
  if (pCur) {
    if (pCur->pCur) {
      tdbDbcClose(pCur->pCur);
    }

    taosMemoryFree(pCur);
  }
#endif
}

static int metaEncodeSchema(void **buf, SSchemaWrapper *pSW) {
  int      tlen = 0;
  SSchema *pSchema;

  tlen += taosEncodeFixedU32(buf, pSW->nCols);
  for (int i = 0; i < pSW->nCols; i++) {
    pSchema = pSW->pSchema + i;
    tlen += taosEncodeFixedI8(buf, pSchema->type);
    tlen += taosEncodeFixedI8(buf, pSchema->flags);
    tlen += taosEncodeFixedI16(buf, pSchema->colId);
    tlen += taosEncodeFixedI32(buf, pSchema->bytes);
    tlen += taosEncodeString(buf, pSchema->name);
  }

  return tlen;
}

static void *metaDecodeSchema(void *buf, SSchemaWrapper *pSW) {
  SSchema *pSchema;

  buf = taosDecodeFixedU32(buf, &pSW->nCols);
  pSW->pSchema = (SSchema *)taosMemoryMalloc(sizeof(SSchema) * pSW->nCols);
  for (int i = 0; i < pSW->nCols; i++) {
    pSchema = pSW->pSchema + i;
    buf = taosDecodeFixedI8(buf, &pSchema->type);
    buf = taosSkipFixedLen(buf, sizeof(int8_t));
    buf = taosDecodeFixedI16(buf, &pSchema->colId);
    buf = taosDecodeFixedI32(buf, &pSchema->bytes);
    buf = taosDecodeStringTo(buf, pSchema->name);
  }

  return buf;
}

static int metaEncodeSchemaEx(void **buf, SSchemaWrapper *pSW) {
  int      tlen = 0;
  SSchema *pSchema;

  tlen += taosEncodeFixedU32(buf, pSW->nCols);
  for (int i = 0; i < pSW->nCols; ++i) {
    pSchema = pSW->pSchema + i;
    tlen += taosEncodeFixedI8(buf, pSchema->type);
    tlen += taosEncodeFixedI8(buf, pSchema->flags);
    tlen += taosEncodeFixedI16(buf, pSchema->colId);
    tlen += taosEncodeFixedI32(buf, pSchema->bytes);
    tlen += taosEncodeString(buf, pSchema->name);
  }

  return tlen;
}

static void *metaDecodeSchemaEx(void *buf, SSchemaWrapper *pSW, bool isGetEx) {
  buf = taosDecodeFixedU32(buf, &pSW->nCols);
  if (isGetEx) {
    pSW->pSchema = (SSchema *)taosMemoryMalloc(sizeof(SSchema) * pSW->nCols);
    for (int i = 0; i < pSW->nCols; i++) {
      SSchema *pSchema = pSW->pSchema + i;
      buf = taosDecodeFixedI8(buf, &pSchema->type);
      buf = taosDecodeFixedI8(buf, &pSchema->flags);
      buf = taosDecodeFixedI16(buf, &pSchema->colId);
      buf = taosDecodeFixedI32(buf, &pSchema->bytes);
      buf = taosDecodeStringTo(buf, pSchema->name);
    }
  } else {
    pSW->pSchema = (SSchema *)taosMemoryMalloc(sizeof(SSchema) * pSW->nCols);
    for (int i = 0; i < pSW->nCols; i++) {
      SSchema *pSchema = pSW->pSchema + i;
      buf = taosDecodeFixedI8(buf, &pSchema->type);
      buf = taosSkipFixedLen(buf, sizeof(int8_t));
      buf = taosDecodeFixedI16(buf, &pSchema->colId);
      buf = taosDecodeFixedI32(buf, &pSchema->bytes);
      buf = taosDecodeStringTo(buf, pSchema->name);
    }
  }

  return buf;
}

static int metaEncodeTbInfo(void **buf, STbCfg *pTbCfg) {
  int tsize = 0;

  tsize += taosEncodeString(buf, pTbCfg->name);
  tsize += taosEncodeFixedU32(buf, pTbCfg->ttl);
  tsize += taosEncodeFixedU32(buf, pTbCfg->keep);
  tsize += taosEncodeFixedU8(buf, pTbCfg->info);

  if (pTbCfg->type == META_SUPER_TABLE) {
    SSchemaWrapper sw = {.nCols = pTbCfg->stbCfg.nTagCols, .pSchema = pTbCfg->stbCfg.pTagSchema};
    tsize += metaEncodeSchema(buf, &sw);
  } else if (pTbCfg->type == META_CHILD_TABLE) {
    tsize += taosEncodeFixedU64(buf, pTbCfg->ctbCfg.suid);
    tsize += tdEncodeKVRow(buf, pTbCfg->ctbCfg.pTag);
  } else if (pTbCfg->type == META_NORMAL_TABLE) {
    // TODO
  } else {
    ASSERT(0);
  }

  return tsize;
}

static void *metaDecodeTbInfo(void *buf, STbCfg *pTbCfg) {
  buf = taosDecodeString(buf, &(pTbCfg->name));
  buf = taosDecodeFixedU32(buf, &(pTbCfg->ttl));
  buf = taosDecodeFixedU32(buf, &(pTbCfg->keep));
  buf = taosDecodeFixedU8(buf, &(pTbCfg->info));

  if (pTbCfg->type == META_SUPER_TABLE) {
    SSchemaWrapper sw;
    buf = metaDecodeSchema(buf, &sw);
    pTbCfg->stbCfg.nTagCols = sw.nCols;
    pTbCfg->stbCfg.pTagSchema = sw.pSchema;
  } else if (pTbCfg->type == META_CHILD_TABLE) {
    buf = taosDecodeFixedU64(buf, &(pTbCfg->ctbCfg.suid));
    buf = tdDecodeKVRow(buf, &(pTbCfg->ctbCfg.pTag));
  } else if (pTbCfg->type == META_NORMAL_TABLE) {
    // TODO
  } else {
    ASSERT(0);
  }
  return buf;
}

int metaCommit(SMeta *pMeta) {
  TXN *pTxn = &pMeta->pDB->txn;

  // Commit current txn
  tdbCommit(pMeta->pDB->pEnv, pTxn);
  tdbTxnClose(pTxn);
  clearPool(pMeta->pDB->pPool);

  // start a new txn
  tdbTxnOpen(&pMeta->pDB->txn, 0, poolMalloc, poolFree, pMeta->pDB->pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);
  tdbBegin(pMeta->pDB->pEnv, pTxn);
  return 0;
}

static SPoolMem *openPool() {
  SPoolMem *pPool = (SPoolMem *)tdbOsMalloc(sizeof(*pPool));

  pPool->prev = pPool->next = pPool;
  pPool->size = 0;

  return pPool;
}

static void clearPool(SPoolMem *pPool) {
  SPoolMem *pMem;

  do {
    pMem = pPool->next;

    if (pMem == pPool) break;

    pMem->next->prev = pMem->prev;
    pMem->prev->next = pMem->next;
    pPool->size -= pMem->size;

    tdbOsFree(pMem);
  } while (1);

  assert(pPool->size == 0);
}

static void closePool(SPoolMem *pPool) {
  clearPool(pPool);
  tdbOsFree(pPool);
}

static void *poolMalloc(void *arg, size_t size) {
  void     *ptr = NULL;
  SPoolMem *pPool = (SPoolMem *)arg;
  SPoolMem *pMem;

  pMem = (SPoolMem *)tdbOsMalloc(sizeof(*pMem) + size);
  if (pMem == NULL) {
    assert(0);
  }

  pMem->size = sizeof(*pMem) + size;
  pMem->next = pPool->next;
  pMem->prev = pPool;

  pPool->next->prev = pMem;
  pPool->next = pMem;
  pPool->size += pMem->size;

  ptr = (void *)(&pMem[1]);
  return ptr;
}

static void poolFree(void *arg, void *ptr) {
  SPoolMem *pPool = (SPoolMem *)arg;
  SPoolMem *pMem;

  pMem = &(((SPoolMem *)ptr)[-1]);

  pMem->next->prev = pMem->prev;
  pMem->prev->next = pMem->next;
  pPool->size -= pMem->size;

  tdbOsFree(pMem);
}

#endif