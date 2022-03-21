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

#define ALLOW_FORBID_FUNC
#include "db.h"

#include "metaDef.h"

#include "tcoding.h"
#include "thash.h"

#define IMPL_WITH_LOCK 1
// #if IMPL_WITH_LOCK
// #endif

typedef struct {
  tb_uid_t uid;
  int32_t  sver;
  int32_t  padding;
} SSchemaKey;

struct SMetaDB {
#if IMPL_WITH_LOCK
  TdThreadRwlock rwlock;
#endif
  // DB
  DB *pTbDB;
  DB *pSchemaDB;
  DB *pSmaDB;

  // IDX
  DB *pNameIdx;
  DB *pStbIdx;
  DB *pNtbIdx;
  DB *pCtbIdx;
  DB *pSmaIdx;
  // ENV
  DB_ENV *pEvn;
};

typedef int (*bdbIdxCbPtr)(DB *, const DBT *, const DBT *, DBT *);

static SMetaDB *metaNewDB();
static void     metaFreeDB(SMetaDB *pDB);
static int      metaOpenBDBEnv(DB_ENV **ppEnv, const char *path);
static void     metaCloseBDBEnv(DB_ENV *pEnv);
static int      metaOpenBDBDb(DB **ppDB, DB_ENV *pEnv, const char *pFName, bool isDup);
static void     metaCloseBDBDb(DB *pDB);
static int      metaOpenBDBIdx(DB **ppIdx, DB_ENV *pEnv, const char *pFName, DB *pDB, bdbIdxCbPtr cbf, bool isDup);
static void     metaCloseBDBIdx(DB *pIdx);
static int      metaNameIdxCb(DB *pIdx, const DBT *pKey, const DBT *pValue, DBT *pSKey);
static int      metaStbIdxCb(DB *pIdx, const DBT *pKey, const DBT *pValue, DBT *pSKey);
static int      metaNtbIdxCb(DB *pIdx, const DBT *pKey, const DBT *pValue, DBT *pSKey);
static int      metaCtbIdxCb(DB *pIdx, const DBT *pKey, const DBT *pValue, DBT *pSKey);
static int      metaSmaIdxCb(DB *pIdx, const DBT *pKey, const DBT *pValue, DBT *pSKey);
static int      metaEncodeTbInfo(void **buf, STbCfg *pTbCfg);
static void    *metaDecodeTbInfo(void *buf, STbCfg *pTbCfg);
static void     metaClearTbCfg(STbCfg *pTbCfg);
static int      metaEncodeSchema(void **buf, SSchemaWrapper *pSW);
static void    *metaDecodeSchema(void *buf, SSchemaWrapper *pSW);
static void     metaDBWLock(SMetaDB *pDB);
static void     metaDBRLock(SMetaDB *pDB);
static void     metaDBULock(SMetaDB *pDB);

#define BDB_PERR(info, code) fprintf(stderr, info " reason: %s", db_strerror(code))

int metaOpenDB(SMeta *pMeta) {
  SMetaDB *pDB;

  // Create DB object
  pDB = metaNewDB();
  if (pDB == NULL) {
    return -1;
  }

  pMeta->pDB = pDB;

  // Open DB Env
  if (metaOpenBDBEnv(&(pDB->pEvn), pMeta->path) < 0) {
    metaCloseDB(pMeta);
    return -1;
  }

  // Open DBs
  if (metaOpenBDBDb(&(pDB->pTbDB), pDB->pEvn, "meta.db", false) < 0) {
    metaCloseDB(pMeta);
    return -1;
  }

  if (metaOpenBDBDb(&(pDB->pSchemaDB), pDB->pEvn, "schema.db", false) < 0) {
    metaCloseDB(pMeta);
    return -1;
  }

  if (metaOpenBDBDb(&(pDB->pSmaDB), pDB->pEvn, "sma.db", false) < 0) {
    metaCloseDB(pMeta);
    return -1;
  }

  // Open Indices
  if (metaOpenBDBIdx(&(pDB->pNameIdx), pDB->pEvn, "name.index", pDB->pTbDB, &metaNameIdxCb, false) < 0) {
    metaCloseDB(pMeta);
    return -1;
  }

  if (metaOpenBDBIdx(&(pDB->pStbIdx), pDB->pEvn, "stb.index", pDB->pTbDB, &metaStbIdxCb, false) < 0) {
    metaCloseDB(pMeta);
    return -1;
  }

  if (metaOpenBDBIdx(&(pDB->pNtbIdx), pDB->pEvn, "ntb.index", pDB->pTbDB, &metaNtbIdxCb, false) < 0) {
    metaCloseDB(pMeta);
    return -1;
  }

  if (metaOpenBDBIdx(&(pDB->pCtbIdx), pDB->pEvn, "ctb.index", pDB->pTbDB, &metaCtbIdxCb, true) < 0) {
    metaCloseDB(pMeta);
    return -1;
  }

  if (metaOpenBDBIdx(&(pDB->pSmaIdx), pDB->pEvn, "sma.index", pDB->pSmaDB, &metaSmaIdxCb, true) < 0) {
    metaCloseDB(pMeta);
    return -1;
  }

  return 0;
}

void metaCloseDB(SMeta *pMeta) {
  if (pMeta->pDB) {
    metaCloseBDBIdx(pMeta->pDB->pSmaIdx);
    metaCloseBDBIdx(pMeta->pDB->pCtbIdx);
    metaCloseBDBIdx(pMeta->pDB->pNtbIdx);
    metaCloseBDBIdx(pMeta->pDB->pStbIdx);
    metaCloseBDBIdx(pMeta->pDB->pNameIdx);
    metaCloseBDBDb(pMeta->pDB->pSmaDB);
    metaCloseBDBDb(pMeta->pDB->pSchemaDB);
    metaCloseBDBDb(pMeta->pDB->pTbDB);
    metaCloseBDBEnv(pMeta->pDB->pEvn);
    metaFreeDB(pMeta->pDB);
    pMeta->pDB = NULL;
  }
}

int metaSaveTableToDB(SMeta *pMeta, STbCfg *pTbCfg) {
  tb_uid_t uid;
  char     buf[512];
  char     buf1[512];
  void    *pBuf;
  DBT      key1, value1;
  DBT      key2, value2;
  SSchema *pSchema = NULL;

  if (pTbCfg->type == META_SUPER_TABLE) {
    uid = pTbCfg->stbCfg.suid;
  } else {
    uid = metaGenerateUid(pMeta);
  }

  {
    // save table info
    pBuf = buf;
    memset(&key1, 0, sizeof(key1));
    memset(&value1, 0, sizeof(key1));

    key1.data = &uid;
    key1.size = sizeof(uid);

    metaEncodeTbInfo(&pBuf, pTbCfg);

    value1.data = buf;
    value1.size = POINTER_DISTANCE(pBuf, buf);
    value1.app_data = pTbCfg;
  }

  // save schema
  uint32_t ncols;
  if (pTbCfg->type == META_SUPER_TABLE) {
    ncols = pTbCfg->stbCfg.nCols;
    pSchema = pTbCfg->stbCfg.pSchema;
  } else if (pTbCfg->type == META_NORMAL_TABLE) {
    ncols = pTbCfg->ntbCfg.nCols;
    pSchema = pTbCfg->ntbCfg.pSchema;
  }

  if (pSchema) {
    pBuf = buf1;
    memset(&key2, 0, sizeof(key2));
    memset(&value2, 0, sizeof(key2));
    SSchemaKey schemaKey = {uid, 0 /*TODO*/, 0};

    key2.data = &schemaKey;
    key2.size = sizeof(schemaKey);

    SSchemaWrapper sw = {.nCols = ncols, .pSchema = pSchema};
    metaEncodeSchema(&pBuf, &sw);

    value2.data = buf1;
    value2.size = POINTER_DISTANCE(pBuf, buf1);
  }

  metaDBWLock(pMeta->pDB);
  pMeta->pDB->pTbDB->put(pMeta->pDB->pTbDB, NULL, &key1, &value1, 0);
  if (pSchema) {
    pMeta->pDB->pSchemaDB->put(pMeta->pDB->pSchemaDB, NULL, &key2, &value2, 0);
  }
  metaDBULock(pMeta->pDB);

  return 0;
}

int metaRemoveTableFromDb(SMeta *pMeta, tb_uid_t uid) {
  // TODO
  return 0;
}

int metaSaveSmaToDB(SMeta *pMeta, STSma *pSmaCfg) {
  // char  buf[512] = {0};  // TODO: may overflow
  void *pBuf = NULL, *qBuf = NULL;
  DBT   key1 = {0}, value1 = {0};

  // save sma info
  int32_t len = tEncodeTSma(NULL, pSmaCfg);
  pBuf = calloc(len, 1);
  if (pBuf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  key1.data = (void *)&pSmaCfg->indexUid;
  key1.size = sizeof(pSmaCfg->indexUid);

  qBuf = pBuf;
  tEncodeTSma(&qBuf, pSmaCfg);

  value1.data = pBuf;
  value1.size = POINTER_DISTANCE(qBuf, pBuf);
  value1.app_data = pSmaCfg;

  metaDBWLock(pMeta->pDB);
  pMeta->pDB->pSmaDB->put(pMeta->pDB->pSmaDB, NULL, &key1, &value1, 0);
  metaDBULock(pMeta->pDB);

  // release
  tfree(pBuf);

  return 0;
}

int metaRemoveSmaFromDb(SMeta *pMeta, const char *indexName) {
  // TODO
#if 0
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

/* ------------------------ STATIC METHODS ------------------------ */
static int metaEncodeSchema(void **buf, SSchemaWrapper *pSW) {
  int      tlen = 0;
  SSchema *pSchema;

  tlen += taosEncodeFixedU32(buf, pSW->nCols);
  for (int i = 0; i < pSW->nCols; i++) {
    pSchema = pSW->pSchema + i;
    tlen += taosEncodeFixedI8(buf, pSchema->type);
    tlen += taosEncodeFixedI32(buf, pSchema->colId);
    tlen += taosEncodeFixedI32(buf, pSchema->bytes);
    tlen += taosEncodeString(buf, pSchema->name);
  }

  return tlen;
}

static void *metaDecodeSchema(void *buf, SSchemaWrapper *pSW) {
  SSchema *pSchema;

  buf = taosDecodeFixedU32(buf, &pSW->nCols);
  pSW->pSchema = (SSchema *)malloc(sizeof(SSchema) * pSW->nCols);
  for (int i = 0; i < pSW->nCols; i++) {
    pSchema = pSW->pSchema + i;
    buf = taosDecodeFixedI8(buf, &pSchema->type);
    buf = taosDecodeFixedI32(buf, &pSchema->colId);
    buf = taosDecodeFixedI32(buf, &pSchema->bytes);
    buf = taosDecodeStringTo(buf, pSchema->name);
  }

  return buf;
}

static SMetaDB *metaNewDB() {
  SMetaDB *pDB = NULL;
  pDB = (SMetaDB *)calloc(1, sizeof(*pDB));
  if (pDB == NULL) {
    return NULL;
  }

#if IMPL_WITH_LOCK
  taosThreadRwlockInit(&pDB->rwlock, NULL);
#endif

  return pDB;
}

static void metaFreeDB(SMetaDB *pDB) {
  if (pDB) {
#if IMPL_WITH_LOCK
    taosThreadRwlockDestroy(&pDB->rwlock);
#endif
    free(pDB);
  }
}

static int metaOpenBDBEnv(DB_ENV **ppEnv, const char *path) {
  int     ret;
  DB_ENV *pEnv;

  if (path == NULL) return 0;

  ret = db_env_create(&pEnv, 0);
  if (ret != 0) {
    BDB_PERR("Failed to create META env", ret);
    return -1;
  }

  ret = pEnv->open(pEnv, path, DB_CREATE | DB_INIT_CDB | DB_INIT_MPOOL, 0);
  if (ret != 0) {
    BDB_PERR("Failed to open META env", ret);
    return -1;
  }

  *ppEnv = pEnv;

  return 0;
}

static void metaCloseBDBEnv(DB_ENV *pEnv) {
  if (pEnv) {
    pEnv->close(pEnv, 0);
  }
}

static int metaOpenBDBDb(DB **ppDB, DB_ENV *pEnv, const char *pFName, bool isDup) {
  int ret;
  DB *pDB;

  ret = db_create(&(pDB), pEnv, 0);
  if (ret != 0) {
    BDB_PERR("Failed to create META DB", ret);
    return -1;
  }

  if (isDup) {
    ret = pDB->set_flags(pDB, DB_DUPSORT);
    if (ret != 0) {
      BDB_PERR("Failed to set DB flags", ret);
      return -1;
    }
  }

  ret = pDB->open(pDB, NULL, pFName, NULL, DB_BTREE, DB_CREATE, 0);
  if (ret) {
    BDB_PERR("Failed to open META DB", ret);
    return -1;
  }

  *ppDB = pDB;

  return 0;
}

static void metaCloseBDBDb(DB *pDB) {
  if (pDB) {
    pDB->close(pDB, 0);
  }
}

static int metaOpenBDBIdx(DB **ppIdx, DB_ENV *pEnv, const char *pFName, DB *pDB, bdbIdxCbPtr cbf, bool isDup) {
  DB *pIdx;
  int ret;

  if (metaOpenBDBDb(ppIdx, pEnv, pFName, isDup) < 0) {
    return -1;
  }

  pIdx = *ppIdx;
  ret = pDB->associate(pDB, NULL, pIdx, cbf, 0);
  if (ret) {
    BDB_PERR("Failed to associate META DB and Index", ret);
  }

  return 0;
}

static void metaCloseBDBIdx(DB *pIdx) {
  if (pIdx) {
    pIdx->close(pIdx, 0);
  }
}

static int metaNameIdxCb(DB *pIdx, const DBT *pKey, const DBT *pValue, DBT *pSKey) {
  STbCfg *pTbCfg = (STbCfg *)(pValue->app_data);

  memset(pSKey, 0, sizeof(*pSKey));

  pSKey->data = pTbCfg->name;
  pSKey->size = strlen(pTbCfg->name);

  return 0;
}

static int metaStbIdxCb(DB *pIdx, const DBT *pKey, const DBT *pValue, DBT *pSKey) {
  STbCfg *pTbCfg = (STbCfg *)(pValue->app_data);

  if (pTbCfg->type == META_SUPER_TABLE) {
    memset(pSKey, 0, sizeof(*pSKey));
    pSKey->data = pKey->data;
    pSKey->size = pKey->size;

    return 0;
  } else {
    return DB_DONOTINDEX;
  }
}

static int metaNtbIdxCb(DB *pIdx, const DBT *pKey, const DBT *pValue, DBT *pSKey) {
  STbCfg *pTbCfg = (STbCfg *)(pValue->app_data);

  if (pTbCfg->type == META_NORMAL_TABLE) {
    memset(pSKey, 0, sizeof(*pSKey));
    pSKey->data = pKey->data;
    pSKey->size = pKey->size;

    return 0;
  } else {
    return DB_DONOTINDEX;
  }
}

static int metaCtbIdxCb(DB *pIdx, const DBT *pKey, const DBT *pValue, DBT *pSKey) {
  STbCfg *pTbCfg = (STbCfg *)(pValue->app_data);
  DBT    *pDbt;

  if (pTbCfg->type == META_CHILD_TABLE) {
    // pDbt = calloc(2, sizeof(DBT));

    // // First key is suid
    // pDbt[0].data = &(pTbCfg->ctbCfg.suid);
    // pDbt[0].size = sizeof(pTbCfg->ctbCfg.suid);

    // // Second key is the first tag
    // void *pTagVal = tdGetKVRowValOfCol(pTbCfg->ctbCfg.pTag, (kvRowColIdx(pTbCfg->ctbCfg.pTag))[0].colId);
    // pDbt[1].data = pTagVal;
    // pDbt[1].size = sizeof(int32_t);

    // Set index key
    memset(pSKey, 0, sizeof(*pSKey));
#if 0
    pSKey->flags = DB_DBT_MULTIPLE | DB_DBT_APPMALLOC;
    pSKey->data = pDbt;
    pSKey->size = 2;
#else
    pSKey->data = &(pTbCfg->ctbCfg.suid);
    pSKey->size = sizeof(pTbCfg->ctbCfg.suid);
#endif

    return 0;
  } else {
    return DB_DONOTINDEX;
  }
}

static int metaSmaIdxCb(DB *pIdx, const DBT *pKey, const DBT *pValue, DBT *pSKey) {
  STSma *pSmaCfg = (STSma *)(pValue->app_data);

  memset(pSKey, 0, sizeof(*pSKey));
  pSKey->data = &(pSmaCfg->tableUid);
  pSKey->size = sizeof(pSmaCfg->tableUid);

  return 0;
}

static int metaEncodeTbInfo(void **buf, STbCfg *pTbCfg) {
  int tsize = 0;

  tsize += taosEncodeString(buf, pTbCfg->name);
  tsize += taosEncodeFixedU32(buf, pTbCfg->ttl);
  tsize += taosEncodeFixedU32(buf, pTbCfg->keep);
  tsize += taosEncodeFixedU8(buf, pTbCfg->type);

  if (pTbCfg->type == META_SUPER_TABLE) {
    SSchemaWrapper sw = {.nCols = pTbCfg->stbCfg.nTagCols, .pSchema = pTbCfg->stbCfg.pTagSchema};
    tsize += metaEncodeSchema(buf, &sw);
  } else if (pTbCfg->type == META_CHILD_TABLE) {
    tsize += taosEncodeFixedU64(buf, pTbCfg->ctbCfg.suid);
    tsize += tdEncodeKVRow(buf, pTbCfg->ctbCfg.pTag);
  } else if (pTbCfg->type == META_NORMAL_TABLE) {
  } else {
    ASSERT(0);
  }

  return tsize;
}

static void *metaDecodeTbInfo(void *buf, STbCfg *pTbCfg) {
  buf = taosDecodeString(buf, &(pTbCfg->name));
  buf = taosDecodeFixedU32(buf, &(pTbCfg->ttl));
  buf = taosDecodeFixedU32(buf, &(pTbCfg->keep));
  buf = taosDecodeFixedU8(buf, &(pTbCfg->type));

  if (pTbCfg->type == META_SUPER_TABLE) {
    SSchemaWrapper sw;
    buf = metaDecodeSchema(buf, &sw);
    pTbCfg->stbCfg.nTagCols = sw.nCols;
    pTbCfg->stbCfg.pTagSchema = sw.pSchema;
  } else if (pTbCfg->type == META_CHILD_TABLE) {
    buf = taosDecodeFixedU64(buf, &(pTbCfg->ctbCfg.suid));
    buf = tdDecodeKVRow(buf, &(pTbCfg->ctbCfg.pTag));
  } else if (pTbCfg->type == META_NORMAL_TABLE) {
  } else {
    ASSERT(0);
  }
  return buf;
}

static void metaClearTbCfg(STbCfg *pTbCfg) {
  tfree(pTbCfg->name);
  if (pTbCfg->type == META_SUPER_TABLE) {
    tdFreeSchema(pTbCfg->stbCfg.pTagSchema);
  } else if (pTbCfg->type == META_CHILD_TABLE) {
    tfree(pTbCfg->ctbCfg.pTag);
  }
}

/* ------------------------ FOR QUERY ------------------------ */
STbCfg *metaGetTbInfoByUid(SMeta *pMeta, tb_uid_t uid) {
  STbCfg  *pTbCfg = NULL;
  SMetaDB *pDB = pMeta->pDB;
  DBT      key = {0};
  DBT      value = {0};
  int      ret;

  // Set key/value
  key.data = &uid;
  key.size = sizeof(uid);

  // Query
  metaDBRLock(pDB);
  ret = pDB->pTbDB->get(pDB->pTbDB, NULL, &key, &value, 0);
  metaDBULock(pDB);
  if (ret != 0) {
    return NULL;
  }

  // Decode
  pTbCfg = (STbCfg *)malloc(sizeof(*pTbCfg));
  if (pTbCfg == NULL) {
    return NULL;
  }

  metaDecodeTbInfo(value.data, pTbCfg);

  return pTbCfg;
}

STbCfg *metaGetTbInfoByName(SMeta *pMeta, char *tbname, tb_uid_t *uid) {
  STbCfg  *pTbCfg = NULL;
  SMetaDB *pDB = pMeta->pDB;
  DBT      key = {0};
  DBT      pkey = {0};
  DBT      pvalue = {0};
  int      ret;

  // Set key/value
  key.data = tbname;
  key.size = strlen(tbname);

  // Query
  metaDBRLock(pDB);
  ret = pDB->pNameIdx->pget(pDB->pNameIdx, NULL, &key, &pkey, &pvalue, 0);
  metaDBULock(pDB);
  if (ret != 0) {
    return NULL;
  }

  // Decode
  *uid = *(tb_uid_t *)(pkey.data);
  pTbCfg = (STbCfg *)malloc(sizeof(*pTbCfg));
  if (pTbCfg == NULL) {
    return NULL;
  }

  metaDecodeTbInfo(pvalue.data, pTbCfg);

  return pTbCfg;
}

STSma *metaGetSmaInfoByIndex(SMeta *pMeta, int64_t indexUid) {
  STSma *  pCfg = NULL;
  SMetaDB *pDB = pMeta->pDB;
  DBT      key = {0};
  DBT      value = {0};
  int      ret;

  // Set key/value
  key.data = (void *)&indexUid;
  key.size = sizeof(indexUid);

  // Query
  metaDBRLock(pDB);
  ret = pDB->pTbDB->get(pDB->pSmaDB, NULL, &key, &value, 0);
  metaDBULock(pDB);
  if (ret != 0) {
    return NULL;
  }

  // Decode
  pCfg = (STSma *)calloc(1, sizeof(STSma));
  if (pCfg == NULL) {
    return NULL;
  }

  if (tDecodeTSma(value.data, pCfg) == NULL) {
    tfree(pCfg);
    return NULL;
  }

  return pCfg;
}

SSchemaWrapper *metaGetTableSchema(SMeta *pMeta, tb_uid_t uid, int32_t sver, bool isinline) {
  uint32_t        nCols;
  SSchemaWrapper *pSW = NULL;
  SMetaDB        *pDB = pMeta->pDB;
  int             ret;
  void           *pBuf;
  SSchema        *pSchema;
  SSchemaKey      schemaKey = {uid, sver, 0};
  DBT             key = {0};
  DBT             value = {0};

  // Set key/value properties
  key.data = &schemaKey;
  key.size = sizeof(schemaKey);

  // Query
  metaDBRLock(pDB);
  ret = pDB->pSchemaDB->get(pDB->pSchemaDB, NULL, &key, &value, 0);
  metaDBULock(pDB);
  if (ret != 0) {
    printf("failed to query schema DB since %s================\n", db_strerror(ret));
    return NULL;
  }

  // Decode the schema
  pBuf = value.data;
  pSW = malloc(sizeof(*pSW));
  metaDecodeSchema(pBuf, pSW);

  return pSW;
}

struct SMTbCursor {
  DBC *pCur;
};

SMTbCursor *metaOpenTbCursor(SMeta *pMeta) {
  SMTbCursor *pTbCur = NULL;
  SMetaDB    *pDB = pMeta->pDB;

  pTbCur = (SMTbCursor *)calloc(1, sizeof(*pTbCur));
  if (pTbCur == NULL) {
    return NULL;
  }

  pDB->pTbDB->cursor(pDB->pTbDB, NULL, &(pTbCur->pCur), 0);

#if 0
    DB_BTREE_STAT *sp;
    pDB->pTbDB->stat(pDB->pTbDB, NULL, &sp, 0);
    printf("**************** %ld\n", sp->bt_nkeys);
#endif

  return pTbCur;
}

void metaCloseTbCursor(SMTbCursor *pTbCur) {
  if (pTbCur) {
    if (pTbCur->pCur) {
      pTbCur->pCur->close(pTbCur->pCur);
    }
    free(pTbCur);
  }
}

char *metaTbCursorNext(SMTbCursor *pTbCur) {
  DBT    key = {0};
  DBT    value = {0};
  STbCfg tbCfg;
  void  *pBuf;

  for (;;) {
    if (pTbCur->pCur->get(pTbCur->pCur, &key, &value, DB_NEXT) == 0) {
      pBuf = value.data;
      metaDecodeTbInfo(pBuf, &tbCfg);
      if (tbCfg.type == META_SUPER_TABLE) {
        free(tbCfg.name);
        free(tbCfg.stbCfg.pTagSchema);
        continue;
      } else if (tbCfg.type == META_CHILD_TABLE) {
        kvRowFree(tbCfg.ctbCfg.pTag);
      }
      return tbCfg.name;
    } else {
      return NULL;
    }
  }
}

STSchema *metaGetTbTSchema(SMeta *pMeta, tb_uid_t uid, int32_t sver) {
  STSchemaBuilder sb;
  STSchema       *pTSchema = NULL;
  SSchema        *pSchema;
  SSchemaWrapper *pSW;
  STbCfg         *pTbCfg;
  tb_uid_t        quid;

  pTbCfg = metaGetTbInfoByUid(pMeta, uid);
  if (pTbCfg->type == META_CHILD_TABLE) {
    quid = pTbCfg->ctbCfg.suid;
  } else {
    quid = uid;
  }

  pSW = metaGetTableSchema(pMeta, quid, sver, true);
  if (pSW == NULL) {
    return NULL;
  }

  // Rebuild a schema
  tdInitTSchemaBuilder(&sb, 0);
  for (int32_t i = 0; i < pSW->nCols; i++) {
    pSchema = pSW->pSchema + i;
    tdAddColToSchema(&sb, pSchema->type, pSchema->colId, pSchema->bytes);
  }
  pTSchema = tdGetSchemaFromBuilder(&sb);
  tdDestroyTSchemaBuilder(&sb);

  return pTSchema;
}

struct SMCtbCursor {
  DBC     *pCur;
  tb_uid_t suid;
};

SMCtbCursor *metaOpenCtbCursor(SMeta *pMeta, tb_uid_t uid) {
  SMCtbCursor *pCtbCur = NULL;
  SMetaDB     *pDB = pMeta->pDB;
  int          ret;

  pCtbCur = (SMCtbCursor *)calloc(1, sizeof(*pCtbCur));
  if (pCtbCur == NULL) {
    return NULL;
  }

  pCtbCur->suid = uid;
  ret = pDB->pCtbIdx->cursor(pDB->pCtbIdx, NULL, &(pCtbCur->pCur), 0);
  if (ret != 0) {
    free(pCtbCur);
    return NULL;
  }

  return pCtbCur;
}

void metaCloseCtbCurosr(SMCtbCursor *pCtbCur) {
  if (pCtbCur) {
    if (pCtbCur->pCur) {
      pCtbCur->pCur->close(pCtbCur->pCur);
    }

    free(pCtbCur);
  }
}

tb_uid_t metaCtbCursorNext(SMCtbCursor *pCtbCur) {
  DBT    skey = {0};
  DBT    pkey = {0};
  DBT    pval = {0};
  void  *pBuf;
  STbCfg tbCfg;

  // Set key
  skey.data = &(pCtbCur->suid);
  skey.size = sizeof(pCtbCur->suid);

  if (pCtbCur->pCur->pget(pCtbCur->pCur, &skey, &pkey, &pval, DB_NEXT) == 0) {
    tb_uid_t id = *(tb_uid_t *)pkey.data;
    assert(id != 0);
    return id;
    //    metaDecodeTbInfo(pBuf, &tbCfg);
    //    return tbCfg.;
  } else {
    return 0;
  }
}

struct SMSmaCursor {
  DBC     *pCur;
  tb_uid_t uid;
};

SMSmaCursor *metaOpenSmaCursor(SMeta *pMeta, tb_uid_t uid) {
  SMSmaCursor *pCur = NULL;
  SMetaDB     *pDB = pMeta->pDB;
  int          ret;

  pCur = (SMSmaCursor *)calloc(1, sizeof(*pCur));
  if (pCur == NULL) {
    return NULL;
  }

  pCur->uid = uid;
  // TODO: lock?
  ret = pDB->pCtbIdx->cursor(pDB->pSmaIdx, NULL, &(pCur->pCur), 0);
  if (ret != 0) {
    free(pCur);
    return NULL;
  }

  return pCur;
}

void metaCloseSmaCurosr(SMSmaCursor *pCur) {
  if (pCur) {
    if (pCur->pCur) {
      pCur->pCur->close(pCur->pCur);
    }

    free(pCur);
  }
}

const char *metaSmaCursorNext(SMSmaCursor *pCur) {
  DBT skey = {0};
  DBT pkey = {0};
  DBT pval = {0};

  // Set key
  skey.data = &(pCur->uid);
  skey.size = sizeof(pCur->uid);
  // TODO: lock?
  if (pCur->pCur->pget(pCur->pCur, &skey, &pkey, &pval, DB_NEXT) == 0) {
    const char *indexName = (const char *)pkey.data;
    assert(indexName != NULL);
    return indexName;
  } else {
    return NULL;
  }
}

STSmaWrapper *metaGetSmaInfoByTable(SMeta *pMeta, tb_uid_t uid) {
  STSmaWrapper *pSW = NULL;

  pSW = calloc(1, sizeof(*pSW));
  if (pSW == NULL) {
    return NULL;
  }

  SMSmaCursor *pCur = metaOpenSmaCursor(pMeta, uid);
  if (pCur == NULL) {
    free(pSW);
    return NULL;
  }

  DBT   skey = {.data = &(pCur->uid), .size = sizeof(pCur->uid)};
  DBT   pval = {0};
  void *pBuf = NULL;

  while (true) {
    // TODO: lock?
    if (pCur->pCur->pget(pCur->pCur, &skey, NULL, &pval, DB_NEXT) == 0) {
      ++pSW->number;
      STSma *tptr = (STSma *)realloc(pSW->tSma, pSW->number * sizeof(STSma));
      if (tptr == NULL) {
        metaCloseSmaCurosr(pCur);
        tdDestroyTSmaWrapper(pSW);
        tfree(pSW);
        return NULL;
      }
      pSW->tSma = tptr;
      pBuf = pval.data;
      if (tDecodeTSma(pBuf, pSW->tSma + pSW->number - 1) == NULL) {
        metaCloseSmaCurosr(pCur);
        tdDestroyTSmaWrapper(pSW);
        tfree(pSW);
        return NULL;
      }
      continue;
    }
    break;
  }

  metaCloseSmaCurosr(pCur);
  
  return pSW;
}

SArray *metaGetSmaTbUids(SMeta *pMeta, bool isDup) {
  SArray * pUids = NULL;
  SMetaDB *pDB = pMeta->pDB;
  DBC *    pCur = NULL;
  DBT      pkey = {0}, pval = {0};
  uint32_t mode = isDup ? DB_NEXT_DUP : DB_NEXT_NODUP;
  int      ret;

  pUids = taosArrayInit(16, sizeof(tb_uid_t));

  if (!pUids) {
    return NULL;
  }

  // TODO: lock?
  ret = pDB->pCtbIdx->cursor(pDB->pSmaIdx, NULL, &pCur, 0);
  if (ret != 0) {
    taosArrayDestroy(pUids);
    return NULL;
  }

  void *pBuf = NULL;

  // TODO: lock?
  while ((ret = pCur->get(pCur, &pkey, &pval, mode)) == 0) {
      taosArrayPush(pUids, pkey.data);
  }

  if (pCur) {
    pCur->close(pCur);
  }

  return pUids;
}

static void metaDBWLock(SMetaDB *pDB) {
#if IMPL_WITH_LOCK
  taosThreadRwlockWrlock(&(pDB->rwlock));
#endif
}

static void metaDBRLock(SMetaDB *pDB) {
#if IMPL_WITH_LOCK
  taosThreadRwlockRdlock(&(pDB->rwlock));
#endif
}

static void metaDBULock(SMetaDB *pDB) {
#if IMPL_WITH_LOCK
  taosThreadRwlockUnlock(&(pDB->rwlock));
#endif
}
