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

#define META_OPEN_DB_IMPL(pDB, options, dir, err) \
  do {                                            \
    pDB = rocksdb_open(options, dir, &err);       \
    if (pDB == NULL) {                            \
      metaCloseDB(pMeta);                         \
      rocksdb_options_destroy(options);           \
      return -1;                                  \
    }                                             \
  } while (0)

int metaOpenDB(SMeta *pMeta) {
  char               dbDir[128];
  char               dir[128];
  char *             err = NULL;
  rocksdb_options_t *options = rocksdb_options_create();

  // TODO
  sprintf(dbDir, "%s/db", pMeta->path);

  if (pMeta->pCache) {
    rocksdb_options_set_row_cache(options, pMeta->pCache);
  }
  rocksdb_options_set_create_if_missing(options, 1);

  pMeta->pDB = (meta_db_t *)calloc(1, sizeof(*(pMeta->pDB)));
  if (pMeta->pDB == NULL) {
    // TODO: handle error
    return -1;
  }

  // tbDb
  sprintf(dir, "%s/tb_db", dbDir);
  META_OPEN_DB_IMPL(pMeta->pDB->tbDb, options, dir, err);

  // nameDb
  sprintf(dir, "%s/name_db", dbDir);
  META_OPEN_DB_IMPL(pMeta->pDB->nameDb, options, dir, err);

  // tagDb
  sprintf(dir, "%s/tag_db", dbDir);
  META_OPEN_DB_IMPL(pMeta->pDB->tagDb, options, dir, err);

  // schemaDb
  sprintf(dir, "%s/schema_db", dbDir);
  META_OPEN_DB_IMPL(pMeta->pDB->schemaDb, options, dir, err);

  // mapDb
  sprintf(dir, "%s/map_db", dbDir);
  META_OPEN_DB_IMPL(pMeta->pDB->mapDb, options, dir, err);

  rocksdb_options_destroy(options);
  return 0;
}

#define META_CLOSE_DB_IMPL(pDB) \
  do {                          \
    if (pDB) {                  \
      rocksdb_close(pDB);       \
      pDB = NULL;               \
    }                           \
  } while (0)

void metaCloseDB(SMeta *pMeta) {
  if (pMeta->pDB) {
    META_CLOSE_DB_IMPL(pMeta->pDB->mapDb);
    META_CLOSE_DB_IMPL(pMeta->pDB->schemaDb);
    META_CLOSE_DB_IMPL(pMeta->pDB->tagDb);
    META_CLOSE_DB_IMPL(pMeta->pDB->nameDb);
    META_CLOSE_DB_IMPL(pMeta->pDB->tbDb);
    free(pMeta->pDB);
    pMeta->pDB = NULL;
  }
}

int metaSaveTableToDB(SMeta *pMeta, const STbOptions *pTbOptions) {
  // TODO
  return 0;
}

int metaRemoveTableFromDb(SMeta *pMeta, tb_uid_t uid) {
  /* TODO */
  return 0;
}