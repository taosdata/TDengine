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

#include "db.h"

#include "metaDef.h"

#include "tcoding.h"
#include "thash.h"

typedef struct {
  tb_uid_t uid;
  int32_t  sver;
} SSchemaKey;

struct SMetaDB {
  // DB
  DB *pTbDB;
  DB *pSchemaDB;
  // IDX
  DB *pNameIdx;
  DB *pStbIdx;
  DB *pNtbIdx;
  DB *pCtbIdx;
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
static int      metaEncodeTbInfo(void **buf, STbCfg *pTbCfg);
static void *   metaDecodeTbInfo(void *buf, STbCfg *pTbCfg);
static void     metaClearTbCfg(STbCfg *pTbCfg);

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

  if (metaOpenBDBDb(&(pDB->pSchemaDB), pDB->pEvn, "meta.db", false) < 0) {
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

  return 0;
}

void metaCloseDB(SMeta *pMeta) {
  if (pMeta->pDB) {
    metaCloseBDBIdx(pMeta->pDB->pCtbIdx);
    metaCloseBDBIdx(pMeta->pDB->pNtbIdx);
    metaCloseBDBIdx(pMeta->pDB->pStbIdx);
    metaCloseBDBIdx(pMeta->pDB->pNameIdx);
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
  void *   pBuf;
  DBT      key, value;
  SSchema *pSchema = NULL;

  if (pTbCfg->type == META_SUPER_TABLE) {
    uid = pTbCfg->stbCfg.suid;
  } else {
    uid = metaGenerateUid(pMeta);
  }

  {
    // save table info
    pBuf = buf;
    memset(&key, 0, sizeof(key));
    memset(&value, 0, sizeof(key));

    key.data = &uid;
    key.size = sizeof(uid);

    metaEncodeTbInfo(&pBuf, pTbCfg);

    value.data = buf;
    value.size = POINTER_DISTANCE(pBuf, buf);
    value.app_data = pTbCfg;

    pMeta->pDB->pTbDB->put(pMeta->pDB->pTbDB, NULL, &key, &value, 0);
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
    pBuf = buf;
    memset(&key, 0, sizeof(key));
    memset(&value, 0, sizeof(key));
    SSchemaKey schemaKey = {uid, 0 /*TODO*/};

    key.data = &schemaKey;
    key.size = sizeof(schemaKey);

    taosEncodeFixedU32(&pBuf, ncols);
    for (size_t i = 0; i < ncols; i++) {
      taosEncodeFixedI8(&pBuf, pSchema[i].type);
      taosEncodeFixedI32(&pBuf, pSchema[i].colId);
      taosEncodeFixedI32(&pBuf, pSchema[i].bytes);
      taosEncodeString(&pBuf, pSchema[i].name);
    }

    value.data = buf;
    value.size = POINTER_DISTANCE(pBuf, buf);

    pMeta->pDB->pSchemaDB->put(pMeta->pDB->pSchemaDB, NULL, &key, &value, 0);
  }

  return 0;
}

int metaRemoveTableFromDb(SMeta *pMeta, tb_uid_t uid) {
  // TODO
  return 0;
}

/* ------------------------ STATIC METHODS ------------------------ */
static SMetaDB *metaNewDB() {
  SMetaDB *pDB = NULL;
  pDB = (SMetaDB *)calloc(1, sizeof(*pDB));
  if (pDB == NULL) {
    return NULL;
  }

  return pDB;
}

static void metaFreeDB(SMetaDB *pDB) {
  if (pDB) {
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

  ret = pEnv->open(pEnv, path, DB_CREATE | DB_INIT_MPOOL, 0);
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
  DBT *   pDbt;

  if (pTbCfg->type == META_CHILD_TABLE) {
    pDbt = calloc(2, sizeof(DBT));

    // First key is suid
    pDbt[0].data = &(pTbCfg->ctbCfg.suid);
    pDbt[0].size = sizeof(pTbCfg->ctbCfg.suid);

    // Second key is the first tag
    void *pTagVal = tdGetKVRowValOfCol(pTbCfg->ctbCfg.pTag, 0);
    pDbt[1].data = varDataVal(pTagVal);
    pDbt[1].size = varDataLen(pTagVal);

    // Set index key
    memset(pSKey, 0, sizeof(*pSKey));
    pSKey->flags = DB_DBT_MULTIPLE | DB_DBT_APPMALLOC;
    pSKey->data = pDbt;
    pSKey->size = 2;

    return 0;
  } else {
    return DB_DONOTINDEX;
  }
}

static int metaEncodeTbInfo(void **buf, STbCfg *pTbCfg) {
  int tsize = 0;

  tsize += taosEncodeString(buf, pTbCfg->name);
  tsize += taosEncodeFixedU32(buf, pTbCfg->ttl);
  tsize += taosEncodeFixedU32(buf, pTbCfg->keep);
  tsize += taosEncodeFixedU8(buf, pTbCfg->type);

  if (pTbCfg->type == META_SUPER_TABLE) {
    tsize += taosEncodeVariantU32(buf, pTbCfg->stbCfg.nTagCols);
    for (uint32_t i = 0; i < pTbCfg->stbCfg.nTagCols; i++) {
      tsize += taosEncodeFixedI8(buf, pTbCfg->stbCfg.pSchema[i].type);
      tsize += taosEncodeFixedI32(buf, pTbCfg->stbCfg.pSchema[i].colId);
      tsize += taosEncodeFixedI32(buf, pTbCfg->stbCfg.pSchema[i].bytes);
      tsize += taosEncodeString(buf, pTbCfg->stbCfg.pSchema[i].name);
    }

    // tsize += tdEncodeSchema(buf, pTbCfg->stbCfg.pTagSchema);
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
    buf = taosDecodeVariantU32(buf, pTbCfg->stbCfg.nTagCols);
    pTbCfg->stbCfg.pTagSchema = (SSchema *)malloc(sizeof(SSchema) * pTbCfg->stbCfg.nTagCols);
    for (uint32_t i = 0; i < pTbCfg->stbCfg.nTagCols; i++) {
      buf = taosDecodeFixedI8(buf, &pTbCfg->stbCfg.pSchema[i].type);
      buf = taosDecodeFixedI32(buf, &pTbCfg->stbCfg.pSchema[i].colId);
      buf = taosDecodeFixedI32(buf, &pTbCfg->stbCfg.pSchema[i].bytes);
      buf = taosDecodeStringTo(buf, pTbCfg->stbCfg.pSchema[i].name);
    }
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