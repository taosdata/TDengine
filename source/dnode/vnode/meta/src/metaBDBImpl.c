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

#include "thash.h"

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

#define META_OPEN_DB(pDB, pEnv, fName)                                     \
  do {                                                                     \
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

  META_OPEN_DB(pDB->pUidIdx, pDB->pEvn, "index.db");

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
  // TODO
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