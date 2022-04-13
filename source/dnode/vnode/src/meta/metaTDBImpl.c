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

#include "tdbInt.h"
typedef struct SPoolMem {
  int64_t          size;
  struct SPoolMem *prev;
  struct SPoolMem *next;
} SPoolMem;

static SPoolMem *openPool();
static void      clearPool(SPoolMem *pPool);
static void      closePool(SPoolMem *pPool);
static void *    poolMalloc(void *arg, size_t size);
static void      poolFree(void *arg, void *ptr);

struct SMetaDB {
  TXN       txn;
  TENV *    pEnv;
  TDB *     pTbDB;
  TDB *     pSchemaDB;
  TDB *     pNameIdx;
  TDB *     pStbIdx;
  TDB *     pNtbIdx;
  TDB *     pCtbIdx;
  SPoolMem *pPool;
};

typedef struct __attribute__((__packed__)) {
  tb_uid_t uid;
  int32_t  sver;
} SSchemaDbKey;

typedef struct {
  char *   name;
  tb_uid_t uid;
} SNameIdxKey;

typedef struct {
  tb_uid_t suid;
  tb_uid_t uid;
} SCtbIdxKey;

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
    tdbDbClose(pMeta->pDB->pCtbIdx);
    tdbDbClose(pMeta->pDB->pNtbIdx);
    tdbDbClose(pMeta->pDB->pStbIdx);
    tdbDbClose(pMeta->pDB->pNameIdx);
    tdbDbClose(pMeta->pDB->pSchemaDB);
    tdbDbClose(pMeta->pDB->pTbDB);
    taosMemoryFree(pMeta->pDB);
  }
}

int metaSaveTableToDB(SMeta *pMeta, STbCfg *pTbCfg) {
  tb_uid_t       uid;
  SMetaDB *      pMetaDb;
  void *         pKey;
  void *         pVal;
  int            kLen;
  int            vLen;
  int            ret;
  char           buf[512];
  void *         pBuf;
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
      schemaWrapper.pSchemaEx = pTbCfg->stbCfg.pSchema;
    } else {
      schemaWrapper.nCols = pTbCfg->ntbCfg.nCols;
      schemaWrapper.pSchemaEx = pTbCfg->ntbCfg.pSchema;
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

STbCfg *metaGetTbInfoByUid(SMeta *pMeta, tb_uid_t uid) {
  int      ret;
  SMetaDB *pMetaDb = pMeta->pDB;
  void *   pKey;
  void *   pVal;
  int      kLen;
  int      vLen;
  STbCfg * pTbCfg;

  // Fetch
  pKey = &uid;
  kLen = sizeof(uid);
  pVal = NULL;
  ret = tdbDbGet(pMetaDb->pTbDB, pKey, kLen, &pVal, &vLen);
  if (ret < 0) {
    return NULL;
  }

  // Decode
  pTbCfg = taosMemoryMalloc(sizeof(*pTbCfg));
  metaDecodeTbInfo(pVal, pTbCfg);

  TDB_FREE(pVal);

  return pTbCfg;
}

STbCfg *metaGetTbInfoByName(SMeta *pMeta, char *tbname, tb_uid_t *uid) {
  void *pKey;
  void *pVal;
  void *ppKey;
  int   pkLen;
  int   kLen;
  int   vLen;
  int   ret;

  pKey = tbname;
  kLen = strlen(tbname) + 1;
  pVal = NULL;
  ppKey = NULL;
  ret = tdbDbPGet(pMeta->pDB->pNameIdx, pKey, kLen, &ppKey, &pkLen, &pVal, &vLen);
  if (ret < 0) {
    return NULL;
  }

  ASSERT(pkLen == kLen + sizeof(uid));

  *uid = *(tb_uid_t *)POINTER_SHIFT(ppKey, kLen);
  TDB_FREE(ppKey);
  TDB_FREE(pVal);

  return metaGetTbInfoByUid(pMeta, *uid);
}

SSchemaWrapper *metaGetTableSchema(SMeta *pMeta, tb_uid_t uid, int32_t sver, bool isinline) {
  return metaGetTableSchemaImpl(pMeta, uid, sver, isinline, false);
}

static SSchemaWrapper *metaGetTableSchemaImpl(SMeta *pMeta, tb_uid_t uid, int32_t sver, bool isinline, bool isGetEx) {
  void *          pKey;
  void *          pVal;
  int             kLen;
  int             vLen;
  int             ret;
  SSchemaDbKey    schemaDbKey;
  SSchemaWrapper *pSchemaWrapper;
  void *          pBuf;

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

STSchema *metaGetTbTSchema(SMeta *pMeta, tb_uid_t uid, int32_t sver) {
  tb_uid_t        quid;
  SSchemaWrapper *pSW;
  STSchemaBuilder sb;
  SSchemaEx *     pSchema;
  STSchema *      pTSchema;
  STbCfg *        pTbCfg;

  pTbCfg = metaGetTbInfoByUid(pMeta, uid);
  if (pTbCfg->type == META_CHILD_TABLE) {
    quid = pTbCfg->ctbCfg.suid;
  } else {
    quid = uid;
  }

  pSW = metaGetTableSchemaImpl(pMeta, quid, sver, true, true);
  if (pSW == NULL) {
    return NULL;
  }

  tdInitTSchemaBuilder(&sb, 0);
  for (int i = 0; i < pSW->nCols; i++) {
    pSchema = pSW->pSchemaEx + i;
    tdAddColToSchema(&sb, pSchema->type, pSchema->sma, pSchema->colId, pSchema->bytes);
  }
  pTSchema = tdGetSchemaFromBuilder(&sb);
  tdDestroyTSchemaBuilder(&sb);

  return pTSchema;
}

struct SMTbCursor {
  TDBC *pDbc;
};

SMTbCursor *metaOpenTbCursor(SMeta *pMeta) {
  SMTbCursor *pTbCur = NULL;
  SMetaDB *   pDB = pMeta->pDB;

  pTbCur = (SMTbCursor *)taosMemoryCalloc(1, sizeof(*pTbCur));
  if (pTbCur == NULL) {
    return NULL;
  }

  tdbDbcOpen(pDB->pTbDB, &pTbCur->pDbc);

  return pTbCur;
}

void metaCloseTbCursor(SMTbCursor *pTbCur) {
  if (pTbCur) {
    if (pTbCur->pDbc) {
      tdbDbcClose(pTbCur->pDbc);
    }
    taosMemoryFree(pTbCur);
  }
}

char *metaTbCursorNext(SMTbCursor *pTbCur) {
  void * pKey = NULL;
  void * pVal = NULL;
  int    kLen;
  int    vLen;
  int    ret;
  void * pBuf;
  STbCfg tbCfg;

  for (;;) {
    ret = tdbDbNext(pTbCur->pDbc, &pKey, &kLen, &pVal, &vLen);
    if (ret < 0) break;
    pBuf = pVal;
    metaDecodeTbInfo(pBuf, &tbCfg);
    if (tbCfg.type == META_SUPER_TABLE) {
      taosMemoryFree(tbCfg.name);
      taosMemoryFree(tbCfg.stbCfg.pTagSchema);
      continue;
      ;
    } else if (tbCfg.type == META_CHILD_TABLE) {
      kvRowFree(tbCfg.ctbCfg.pTag);
    }

    return tbCfg.name;
  }

  return NULL;
}

struct SMCtbCursor {
  TDBC *   pCur;
  tb_uid_t suid;
  void *   pKey;
  void *   pVal;
  int      kLen;
  int      vLen;
};

SMCtbCursor *metaOpenCtbCursor(SMeta *pMeta, tb_uid_t uid) {
  SMCtbCursor *pCtbCur = NULL;
  SMetaDB *    pDB = pMeta->pDB;
  int          ret;

  pCtbCur = (SMCtbCursor *)taosMemoryCalloc(1, sizeof(*pCtbCur));
  if (pCtbCur == NULL) {
    return NULL;
  }

  pCtbCur->suid = uid;
  ret = tdbDbcOpen(pDB->pCtbIdx, &pCtbCur->pCur);
  if (ret < 0) {
    taosMemoryFree(pCtbCur);
    return NULL;
  }

  // TODO: move the cursor to the suid there

  return pCtbCur;
}

void metaCloseCtbCurosr(SMCtbCursor *pCtbCur) {
  if (pCtbCur) {
    if (pCtbCur->pCur) {
      tdbDbcClose(pCtbCur->pCur);

      TDB_FREE(pCtbCur->pKey);
      TDB_FREE(pCtbCur->pVal);
    }

    taosMemoryFree(pCtbCur);
  }
}

tb_uid_t metaCtbCursorNext(SMCtbCursor *pCtbCur) {
  int         ret;
  SCtbIdxKey *pCtbIdxKey;

  ret = tdbDbNext(pCtbCur->pCur, &pCtbCur->pKey, &pCtbCur->kLen, &pCtbCur->pVal, &pCtbCur->vLen);
  if (ret < 0) {
    return 0;
  }

  pCtbIdxKey = pCtbCur->pKey;

  return pCtbIdxKey->uid;
}

int metaGetTbNum(SMeta *pMeta) {
  // TODO
  // ASSERT(0);
  return 0;
}

STSmaWrapper *metaGetSmaInfoByTable(SMeta *pMeta, tb_uid_t uid) {
  // TODO
  ASSERT(0);
  return NULL;
}

int metaRemoveSmaFromDb(SMeta *pMeta, int64_t indexUid) {
  // TODO
  ASSERT(0);
  return 0;
}

int metaSaveSmaToDB(SMeta *pMeta, STSma *pSmaCfg) {
  // TODO
  ASSERT(0);
  return 0;
}

STSma *metaGetSmaInfoByIndex(SMeta *pMeta, int64_t indexUid) {
  // TODO
  ASSERT(0);
  return NULL;
}

const char *metaSmaCursorNext(SMSmaCursor *pCur) {
  // TODO
  ASSERT(0);
  return NULL;
}

void metaCloseSmaCurosr(SMSmaCursor *pCur) {
  // TODO
  ASSERT(0);
}

SArray *metaGetSmaTbUids(SMeta *pMeta, bool isDup) {
  // TODO
  // ASSERT(0); // comment this line to pass CI
  return NULL;
}

SMSmaCursor *metaOpenSmaCursor(SMeta *pMeta, tb_uid_t uid) {
  // TODO
  ASSERT(0);
  return NULL;
}

static int metaEncodeSchema(void **buf, SSchemaWrapper *pSW) {
  int      tlen = 0;
  SSchema *pSchema;

  tlen += taosEncodeFixedU32(buf, pSW->nCols);
  for (int i = 0; i < pSW->nCols; i++) {
    pSchema = pSW->pSchema + i;
    tlen += taosEncodeFixedI8(buf, pSchema->type);
    tlen += taosEncodeFixedI8(buf, pSchema->index);
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
  int        tlen = 0;
  SSchemaEx *pSchema;

  tlen += taosEncodeFixedU32(buf, pSW->nCols);
  for (int i = 0; i < pSW->nCols; ++i) {
    pSchema = pSW->pSchemaEx + i;
    tlen += taosEncodeFixedI8(buf, pSchema->type);
    tlen += taosEncodeFixedI8(buf, pSchema->sma);
    tlen += taosEncodeFixedI16(buf, pSchema->colId);
    tlen += taosEncodeFixedI32(buf, pSchema->bytes);
    tlen += taosEncodeString(buf, pSchema->name);
  }

  return tlen;
}

static void *metaDecodeSchemaEx(void *buf, SSchemaWrapper *pSW, bool isGetEx) {
  buf = taosDecodeFixedU32(buf, &pSW->nCols);
  if (isGetEx) {
    pSW->pSchemaEx = (SSchemaEx *)taosMemoryMalloc(sizeof(SSchemaEx) * pSW->nCols);
    for (int i = 0; i < pSW->nCols; i++) {
      SSchemaEx *pSchema = pSW->pSchemaEx + i;
      buf = taosDecodeFixedI8(buf, &pSchema->type);
      buf = taosDecodeFixedI8(buf, &pSchema->sma);
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
  void *    ptr = NULL;
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
