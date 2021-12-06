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
  DB *    pDB;
  DB *    pIdx;
  DB_ENV *pEvn;
};

int metaOpenDB(SMeta *pMeta) {
  int  ret;
  char dbname[128];

  pMeta->pDB = (SMetaDB *)calloc(1, sizeof(SMetaDB));
  if (pMeta->pDB == NULL) {
    // TODO: handle error
    return -1;
  }

  // TODO: create the env

  ret = db_create(&(pMeta->pDB->pDB), pMeta->pDB->pEvn, 0);
  if (ret != 0) {
    // TODO: handle error
    return -1;
  }

  ret = db_create(&(pMeta->pDB->pIdx), pMeta->pDB->pEvn, 0);
  if (ret != 0) {
    // TODO: handle error
    return -1;
  }

  ret = pMeta->pDB->pDB->open(pMeta->pDB->pDB, /* DB structure pointer */
                              NULL,            /* Transaction pointer */
                              "meta.db",       /* On-disk file that holds the database */
                              NULL,            /* Optional logical database name */
                              DB_BTREE,        /* Database access method */
                              DB_CREATE,       /* Open flags */
                              0);              /* File mode */
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
    /* TODO */
    free(pMeta->pDB);
  }
}

int metaSaveTableToDB(SMeta *pMeta, const STbCfg *pTbOptions) {
  // TODO
  return 0;
}

int metaRemoveTableFromDb(SMeta *pMeta, tb_uid_t uid) {
  // TODO
}