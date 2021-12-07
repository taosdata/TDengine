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
  DB *      pStbDB;
  DB *      pNtbDB;
  SHashObj *pCtbMap;
  DB *      pSchemaDB;
  // IDX
  SHashObj *pIdxMap;
  DB *      pNameIdx;
  DB *      pUidIdx;
  // ENV
  DB_ENV *pEvn;
};

#define P_ERROR(info, code) fprintf(stderr, info "reason: %s", db_strerror(code))

static SMetaDB *metaNewDB();
static void     metaFreeDB(SMetaDB *pDB);
static int      metaCreateDBEnv(SMetaDB *pDB, const char *path);
static void     metaDestroyDBEnv(SMetaDB *pDB);
static int      metaEncodeSchemaKey(void **buf, SSchemaKey *pSchemaKey);
static void *   metaDecodeSchemaKey(void *buf, SSchemaKey *pSchemaKey);
static int      metaNameIdxCb(DB *sdbp, const DBT *pKey, const DBT *pValue, DBT *pSKey);
static int      metaUidIdxCb(DB *sdbp, const DBT *pKey, const DBT *pValue, DBT *pSKey);
static void     metaPutSchema(SMeta *pMeta, tb_uid_t uid, STSchema *pSchema);
static int      metaEncodeTbInfo(void **buf, STbCfg *pTbCfg);
static void *   metaDecodeTbInfo(void *buf, STbCfg *pTbCfg);
static int      metaSaveTbInfo(DB *pDB, tb_uid_t uid, STbCfg *pTbCfg);

#define META_OPEN_DB(pDB, pEnv, fName)                                     \
  do {                                                                     \
    int ret;                                                               \
    ret = db_create(&((pDB)), (pEnv), 0);                                  \
    if (ret != 0) {                                                        \
      P_ERROR("Failed to create META DB", ret);                            \
      metaCloseDB(pMeta);                                                  \
      return -1;                                                           \
    }                                                                      \
                                                                           \
    ret = (pDB)->open((pDB), NULL, (fName), NULL, DB_BTREE, DB_CREATE, 0); \
    if (ret != 0) {                                                        \
      P_ERROR("Failed to open META DB", ret);                              \
      metaCloseDB(pMeta);                                                  \
      return -1;                                                           \
    }                                                                      \
  } while (0)

#define META_CLOSE_DB(pDB)

#define META_ASSOCIATE_IDX(pDB, pIdx, cbf)                     \
  do {                                                         \
    int ret = (pDB)->associate((pDB), NULL, (pIdx), (cbf), 0); \
    if (ret != 0) {                                            \
      P_ERROR("Failed to associate META DB", ret);             \
      metaCloseDB(pMeta);                                      \
    }                                                          \
  } while (0)

int metaOpenDB(SMeta *pMeta) {
  int      ret;
  SMetaDB *pDB;

  pMeta->pDB = metaNewDB();
  if (pMeta->pDB == NULL) {
    return -1;
  }

  pDB = pMeta->pDB;

  if (metaCreateDBEnv(pDB, pMeta->path) < 0) {
    metaCloseDB(pMeta);
    return -1;
  }

  META_OPEN_DB(pDB->pStbDB, pDB->pEvn, "meta.db");

  META_OPEN_DB(pDB->pNtbDB, pDB->pEvn, "meta.db");

  META_OPEN_DB(pDB->pSchemaDB, pDB->pEvn, "meta.db");

  {
    // TODO: Loop to open each super table db
  }

  META_OPEN_DB(pDB->pNameIdx, pDB->pEvn, "index.db");

  // META_OPEN_DB(pDB->pUidIdx, pDB->pEvn, "index.db");

  // Associate name index
  META_ASSOCIATE_IDX(pDB->pStbDB, pDB->pNameIdx, metaNameIdxCb);
  // META_ASSOCIATE_IDX(pDB->pStbDB, pDB->pUidIdx, metaUidIdxCb);
  // META_ASSOCIATE_IDX(pDB->pNtbDB, pDB->pNameIdx, metaNameIdxCb);
  // META_ASSOCIATE_IDX(pDB->pNtbDB, pDB->pUidIdx, metaUidIdxCb);

  for (;;) {
    // Loop to associate each super table db
    break;
  }

  {
    // TODO: Loop to open index DB for each super table
    // and create the association between main DB and index
  }

  return 0;
}

void metaCloseDB(SMeta *pMeta) {
  if (pMeta->pDB) {
    metaDestroyDBEnv(pMeta->pDB);
    metaFreeDB(pMeta->pDB);
    pMeta->pDB = NULL;
  }
}

int metaSaveTableToDB(SMeta *pMeta, STbCfg *pTbCfg) {
  char       buf[512];
  void *     pBuf;
  DBT        key = {0};
  DBT        value = {0};
  SSchemaKey schemaKey;
  tb_uid_t   uid;

  if (pTbCfg->type == META_SUPER_TABLE) {
    // Handle SUPER table
    uid = pTbCfg->stbCfg.suid;

    // Same table info
    metaSaveTbInfo(pMeta->pDB->pStbDB, uid, pTbCfg);

    // save schema
    metaPutSchema(pMeta, uid, pTbCfg->stbCfg.pSchema);

    {
      // Create a super table DB and corresponding index DB
      DB *pStbDB;
      DB *pStbIdxDB;

      META_OPEN_DB(pStbDB, pMeta->pDB->pEvn, "meta.db");

      META_OPEN_DB(pStbIdxDB, pMeta->pDB->pEvn, "index.db");

      // TODO META_ASSOCIATE_IDX();
    }
  } else if (pTbCfg->type == META_CHILD_TABLE) {
    // Handle CHILD table
    uid = metaGenerateUid(pMeta);

    DB *pCTbDB = taosHashGet(pMeta->pDB->pCtbMap, &(pTbCfg->ctbCfg.suid), sizeof(pTbCfg->ctbCfg.suid));
    if (pCTbDB == NULL) {
      ASSERT(0);
    }

    metaSaveTbInfo(pCTbDB, uid, pTbCfg);

  } else if (pTbCfg->type == META_NORMAL_TABLE) {
    // Handle NORMAL table
    uid = metaGenerateUid(pMeta);

    metaSaveTbInfo(pMeta->pDB->pNtbDB, uid, pTbCfg);

    metaPutSchema(pMeta, uid, pTbCfg->stbCfg.pSchema);
  } else {
    ASSERT(0);
  }

  return 0;
}

int metaRemoveTableFromDb(SMeta *pMeta, tb_uid_t uid) {
  // TODO
}

/* ------------------------ STATIC METHODS ------------------------ */
static SMetaDB *metaNewDB() {
  SMetaDB *pDB;
  pDB = (SMetaDB *)calloc(1, sizeof(*pDB));
  if (pDB == NULL) {
    return NULL;
  }

  pDB->pCtbMap = taosHashInit(0, MurmurHash3_32, false, HASH_NO_LOCK);
  if (pDB->pCtbMap == NULL) {
    metaFreeDB(pDB);
    return NULL;
  }

  pDB->pIdxMap = taosHashInit(0, MurmurHash3_32, false, HASH_NO_LOCK);
  if (pDB->pIdxMap == NULL) {
    metaFreeDB(pDB);
    return NULL;
  }

  return pDB;
}

static void metaFreeDB(SMetaDB *pDB) {
  if (pDB == NULL) {
    if (pDB->pIdxMap) {
      taosHashCleanup(pDB->pIdxMap);
    }

    if (pDB->pCtbMap) {
      taosHashCleanup(pDB->pCtbMap);
    }

    free(pDB);
  }
}

static int metaCreateDBEnv(SMetaDB *pDB, const char *path) {
  int ret;

  if (path == NULL) return 0;

  ret = db_env_create(&(pDB->pEvn), 0);
  if (ret != 0) {
    P_ERROR("Failed to create META DB ENV", ret);
    return -1;
  }

  ret = pDB->pEvn->open(pDB->pEvn, path, DB_CREATE | DB_INIT_MPOOL, 0);
  if (ret != 0) {
    P_ERROR("failed to open META DB ENV", ret);
    return -1;
  }

  return 0;
}

static void metaDestroyDBEnv(SMetaDB *pDB) {
  if (pDB->pEvn) {
    pDB->pEvn->close(pDB->pEvn, 0);
  }
}

static int metaEncodeSchemaKey(void **buf, SSchemaKey *pSchemaKey) {
  int tsize = 0;

  tsize += taosEncodeFixedU64(buf, pSchemaKey->uid);
  tsize += taosEncodeFixedI32(buf, pSchemaKey->sver);

  return tsize;
}

static void *metaDecodeSchemaKey(void *buf, SSchemaKey *pSchemaKey) {
  buf = taosDecodeFixedU64(buf, &(pSchemaKey->uid));
  buf = taosDecodeFixedI32(buf, &(pSchemaKey->sver));

  return buf;
}

static int metaNameIdxCb(DB *sdbp, const DBT *pKey, const DBT *pValue, DBT *pSKey) {
  // TODO
  return 0;
}

static int metaUidIdxCb(DB *sdbp, const DBT *pKey, const DBT *pValue, DBT *pSKey) {
  // TODO
  return 0;
}

static void metaPutSchema(SMeta *pMeta, tb_uid_t uid, STSchema *pSchema) {
  SSchemaKey skey;
  char       buf[256];
  void *     pBuf = buf;
  DBT        key = {0};
  DBT        value = {0};

  skey.uid = uid;
  skey.sver = schemaVersion(pSchema);

  key.data = &skey;
  key.size = sizeof(skey);

  tdEncodeSchema(&pBuf, pSchema);
  value.data = buf;
  value.size = POINTER_DISTANCE(pBuf, buf);

  pMeta->pDB->pSchemaDB->put(pMeta->pDB->pSchemaDB, NULL, &key, &value, 0);
}

static int metaEncodeTbInfo(void **buf, STbCfg *pTbCfg) {
  int tsize = 0;

  tsize += taosEncodeString(buf, pTbCfg->name);
  tsize += taosEncodeFixedU32(buf, pTbCfg->ttl);
  tsize += taosEncodeFixedU32(buf, pTbCfg->keep);

  if (pTbCfg->type == META_SUPER_TABLE) {
    tsize += tdEncodeSchema(buf, pTbCfg->stbCfg.pTagSchema);
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
  // TODO
  buf = taosDecodeString(buf, &(pTbCfg->name));
  buf = taosDecodeFixedU32(buf, &(pTbCfg->ttl));
  buf = taosDecodeFixedU32(buf, &(pTbCfg->keep));

  if (pTbCfg->type == META_SUPER_TABLE) {
    buf = tdDecodeSchema(buf, &(pTbCfg->stbCfg.pTagSchema));
  } else if (pTbCfg->type == META_CHILD_TABLE) {
    buf = taosDecodeFixedU64(buf, &(pTbCfg->ctbCfg.suid));
    buf = tdDecodeKVRow(buf, &(pTbCfg->ctbCfg.pTag));
  } else if (pTbCfg->type == META_NORMAL_TABLE) {
  } else {
    ASSERT(0);
  }
  return buf;
}

static int metaSaveTbInfo(DB *pDB, tb_uid_t uid, STbCfg *pTbCfg) {
  DBT   key = {0};
  DBT   value = {0};
  char  buf[512];
  void *pBuf = buf;

  key.data = &uid;
  key.size = sizeof(uid);

  metaEncodeTbInfo(&pBuf, pTbCfg);

  value.data = buf;
  value.size = POINTER_DISTANCE(pBuf, buf);

  pDB->put(pDB, NULL, &key, &value, 0);

  return 0;
}