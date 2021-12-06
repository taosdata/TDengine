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

#include "metaDef.h"

#include "db.h"

struct SMetaDB {
  DB *    pStbDB;
  DB *    pCtbDB;
  DB *    pNtbDB;
  DB *    pIdx;
  DB_ENV *pEvn;
};

int metaOpenDB(SMeta *pMeta) {
  int ret;

  pMeta->pDB = (SMetaDB *)calloc(1, sizeof(SMetaDB));
  if (pMeta->pDB == NULL) {
    // TODO: handle error
    return -1;
  }

  // create the env
  ret = db_env_create(&(pMeta->pDB->pEvn), 0);
  if (ret != 0) {
    // TODO: handle error
    return -1;
  }

  ret = pMeta->pDB->pEvn->open(pMeta->pDB->pEvn, pMeta->path, DB_CREATE | DB_INIT_MPOOL, 0);
  if (ret != 0) {
    // TODO: handle error
    return -1;
  }

  ret = db_create(&(pMeta->pDB->pStbDB), pMeta->pDB->pEvn, 0);
  if (ret != 0) {
    // TODO: handle error
    return -1;
  }

  ret = db_create(&(pMeta->pDB->pCtbDB), pMeta->pDB->pEvn, 0);
  if (ret != 0) {
    // TODO: handle error
    return -1;
  }

  ret = db_create(&(pMeta->pDB->pNtbDB), pMeta->pDB->pEvn, 0);
  if (ret != 0) {
    // TODO: handle error
    return -1;
  }

  ret = db_create(&(pMeta->pDB->pIdx), pMeta->pDB->pEvn, 0);
  if (ret != 0) {
    // TODO: handle error
    return -1;
  }

  ret = pMeta->pDB->pStbDB->open(pMeta->pDB->pStbDB, /* DB structure pointer */
                                 NULL,               /* Transaction pointer */
                                 "meta.db",          /* On-disk file that holds the database */
                                 NULL,               /* Optional logical database name */
                                 DB_BTREE,           /* Database access method */
                                 DB_CREATE,          /* Open flags */
                                 0);                 /* File mode */
  if (ret != 0) {
    // TODO: handle error
    return -1;
  }

  ret = pMeta->pDB->pCtbDB->open(pMeta->pDB->pCtbDB, /* DB structure pointer */
                                 NULL,               /* Transaction pointer */
                                 "meta.db",          /* On-disk file that holds the database */
                                 NULL,               /* Optional logical database name */
                                 DB_BTREE,           /* Database access method */
                                 DB_CREATE,          /* Open flags */
                                 0);                 /* File mode */
  if (ret != 0) {
    // TODO: handle error
    return -1;
  }

  ret = pMeta->pDB->pNtbDB->open(pMeta->pDB->pNtbDB, /* DB structure pointer */
                                 NULL,               /* Transaction pointer */
                                 "meta.db",          /* On-disk file that holds the database */
                                 NULL,               /* Optional logical database name */
                                 DB_BTREE,           /* Database access method */
                                 DB_CREATE,          /* Open flags */
                                 0);                 /* File mode */
  if (ret != 0) {
    // TODO: handle error
    return -1;
  }

  ret = pMeta->pDB->pIdx->open(pMeta->pDB->pIdx, /* DB structure pointer */
                               NULL,             /* Transaction pointer */
                               "index.db",       /* On-disk file that holds the database */
                               NULL,             /* Optional logical database name */
                               DB_BTREE,         /* Database access method */
                               DB_CREATE,        /* Open flags */
                               0);               /* File mode */
  if (ret != 0) {
    // TODO: handle error
    return -1;
  }

  // TODO
  return 0;
}

void metaCloseDB(SMeta *pMeta) {
  if (pMeta->pDB) {
    if (pMeta->pDB->pIdx) {
      pMeta->pDB->pIdx->close(pMeta->pDB->pIdx, 0);
      pMeta->pDB->pIdx = NULL;
    }

    if (pMeta->pDB->pNtbDB) {
      pMeta->pDB->pNtbDB->close(pMeta->pDB->pNtbDB, 0);
      pMeta->pDB->pNtbDB = NULL;
    }

    if (pMeta->pDB->pCtbDB) {
      pMeta->pDB->pCtbDB->close(pMeta->pDB->pCtbDB, 0);
      pMeta->pDB->pCtbDB = NULL;
    }

    if (pMeta->pDB->pStbDB) {
      pMeta->pDB->pStbDB->close(pMeta->pDB->pStbDB, 0);
      pMeta->pDB->pStbDB = NULL;
    }

    if (pMeta->pDB->pEvn) {
      pMeta->pDB->pEvn->close(pMeta->pDB->pEvn, 0);
      pMeta->pDB->pEvn = NULL;
    }

    free(pMeta->pDB);
  }
}

int metaSaveTableToDB(SMeta *pMeta, const STbCfg *pTbCfg) {
  tb_uid_t uid;
  DBT      key = {0};
  DBT      value = {0};
  char     buf[256];
  void *   pBuf;
  int      bsize;

  if (pTbCfg->type == META_SUPER_TABLE) {
    uid = pTbCfg->stbCfg.suid;
  } else {
    uid = metaGenerateUid(pMeta);
  }

  key.size = sizeof(uid);
  key.data = &uid;

  pBuf = buf;
  value.size = metaEncodeTbCfg(&pBuf, pTbCfg);
  value.data = buf;

  pMeta->pDB->pStbDB->put(pMeta->pDB->pStbDB, NULL, &key, &value, 0);

  return 0;
}

int metaRemoveTableFromDb(SMeta *pMeta, tb_uid_t uid) {
  // TODO
}