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

#if !USE_SQLITE_IMPL
static void metaSaveSchemaDB(SMeta *pMeta, tb_uid_t uid, STSchema *pSchema);
static void metaGetSchemaDBKey(char key[], tb_uid_t uid, int sversion);

#define SCHEMA_KEY_LEN (sizeof(tb_uid_t) + sizeof(int))

#define META_OPEN_DB_IMPL(pDB, options, dir, err) \
  do {                                            \
    pDB = rocksdb_open(options, dir, &err);       \
    if (pDB == NULL) {                            \
      metaCloseDB(pMeta);                         \
      rocksdb_options_destroy(options);           \
      return -1;                                  \
    }                                             \
  } while (0)

#define META_CLOSE_DB_IMPL(pDB) \
  do {                          \
    if (pDB) {                  \
      rocksdb_close(pDB);       \
      pDB = NULL;               \
    }                           \
  } while (0)
#endif

int metaOpenDB(SMeta *pMeta) {
  char  dir[128];
  int   rc;
  char *err = NULL;
#if !USE_SQLITE_IMPL
  rocksdb_options_t *options = rocksdb_options_create();

  if (pMeta->pCache) {
    rocksdb_options_set_row_cache(options, pMeta->pCache);
  }
  rocksdb_options_set_create_if_missing(options, 1);

  pMeta->pDB = (meta_db_t *)calloc(1, sizeof(*(pMeta->pDB)));
  if (pMeta->pDB == NULL) {
    // TODO: handle error
    rocksdb_options_destroy(options);
    return -1;
  }

  // tbDb
  sprintf(dir, "%s/tb_db", pMeta->path);
  META_OPEN_DB_IMPL(pMeta->pDB->tbDb, options, dir, err);

  // nameDb
  sprintf(dir, "%s/name_db", pMeta->path);
  META_OPEN_DB_IMPL(pMeta->pDB->nameDb, options, dir, err);

  // tagDb
  sprintf(dir, "%s/tag_db", pMeta->path);
  META_OPEN_DB_IMPL(pMeta->pDB->tagDb, options, dir, err);

  // schemaDb
  sprintf(dir, "%s/schema_db", pMeta->path);
  META_OPEN_DB_IMPL(pMeta->pDB->schemaDb, options, dir, err);

  // mapDb
  sprintf(dir, "%s/meta.db", pMeta->path);
  if (sqlite3_open(dir, &(pMeta->pDB->mapDb)) != SQLITE_OK) {
    // TODO
  }

  // // set read uncommitted
  sqlite3_exec(pMeta->pDB->mapDb, "PRAGMA read_uncommitted=true;", 0, 0, 0);
  sqlite3_exec(pMeta->pDB->mapDb, "BEGIN;", 0, 0, 0);

  rocksdb_options_destroy(options);
#else
  sprintf(dir, "%s/meta.db", pMeta->path);
  rc = sqlite3_open(dir, &(pMeta->pDB));
  if (rc != SQLITE_OK) {
    // TODO: handle error
    printf("failed to open meta.db\n");
  }

  // For all tables
  rc = sqlite3_exec(pMeta->pDB,
                    "CREATE TABLE IF NOT EXISTS tb ("
                    "  tbname VARCHAR(256) NOT NULL UNIQUE,"
                    "  tb_uid INTEGER NOT NULL UNIQUE "
                    ");",
                    NULL, NULL, &err);
  if (rc != SQLITE_OK) {
    // TODO: handle error
    printf("failed to create meta table tb since %s\n", err);
  }

  // For super tables
  rc = sqlite3_exec(pMeta->pDB,
                    "CREATE TABLE IF NOT EXISTS stb ("
                    "    tb_uid INTEGER NOT NULL UNIQUE,"
                    "    tbname VARCHAR(256) NOT NULL UNIQUE,"
                    "    tb_schema BLOB NOT NULL,"
                    "    tag_schema BLOB NOT NULL"
                    ");",
                    NULL, NULL, &err);
  if (rc != SQLITE_OK) {
    // TODO: handle error
    printf("failed to create meta table stb since %s\n", err);
  }

  // For normal tables
  rc = sqlite3_exec(pMeta->pDB,
                    "CREATE TABLE IF NOT EXISTS ntb ("
                    "    tb_uid INTEGER NOT NULL UNIQUE,"
                    "    tbname VARCHAR(256) NOT NULL,"
                    "    tb_schema BLOB NOT NULL"
                    ");",
                    NULL, NULL, &err);
  if (rc != SQLITE_OK) {
    // TODO: handle error
    printf("failed to create meta table ntb since %s\n", err);
  }

  sqlite3_exec(pMeta->pDB, "BEGIN;", NULL, NULL, &err);

  tfree(err);
#endif

  return 0;
}

void metaCloseDB(SMeta *pMeta) {
#if !USE_SQLITE_IMPL
  if (pMeta->pDB) {
    if (pMeta->pDB->mapDb) {
      sqlite3_exec(pMeta->pDB->mapDb, "COMMIT;", 0, 0, 0);
      sqlite3_close(pMeta->pDB->mapDb);
      pMeta->pDB->mapDb = NULL;
    }

    META_CLOSE_DB_IMPL(pMeta->pDB->schemaDb);
    META_CLOSE_DB_IMPL(pMeta->pDB->tagDb);
    META_CLOSE_DB_IMPL(pMeta->pDB->nameDb);
    META_CLOSE_DB_IMPL(pMeta->pDB->tbDb);
    free(pMeta->pDB);
    pMeta->pDB = NULL;
  }
#else
  if (pMeta->pDB) {
    sqlite3_exec(pMeta->pDB, "BEGIN;", NULL, NULL, NULL);
    sqlite3_close(pMeta->pDB);
    pMeta->pDB = NULL;
  }

  // TODO
#endif
}

int metaSaveTableToDB(SMeta *pMeta, const STbCfg *pTbOptions) {
#if !USE_SQLITE_IMPL
  tb_uid_t uid;
  char *   err = NULL;
  size_t   size;
  char     pBuf[1024];  // TODO
  char     sql[128];

  rocksdb_writeoptions_t *wopt = rocksdb_writeoptions_create();

  // Generate a uid for child and normal table
  if (pTbOptions->type == META_SUPER_TABLE) {
    uid = pTbOptions->stbCfg.suid;
  } else {
    uid = metaGenerateUid(pMeta);
  }

  // Save tbname -> uid to tbnameDB
  rocksdb_put(pMeta->pDB->nameDb, wopt, pTbOptions->name, strlen(pTbOptions->name), (char *)(&uid), sizeof(uid), &err);
  rocksdb_writeoptions_disable_WAL(wopt, 1);

  // Save uid -> tb_obj to tbDB
  size = metaEncodeTbObjFromTbOptions(pTbOptions, pBuf, 1024);
  rocksdb_put(pMeta->pDB->tbDb, wopt, (char *)(&uid), sizeof(uid), pBuf, size, &err);

  switch (pTbOptions->type) {
    case META_NORMAL_TABLE:
      // save schemaDB
      metaSaveSchemaDB(pMeta, uid, pTbOptions->ntbCfg.pSchema);
      break;
    case META_SUPER_TABLE:
      // save schemaDB
      metaSaveSchemaDB(pMeta, uid, pTbOptions->stbCfg.pSchema);

      // // save mapDB (really need?)
      // rocksdb_put(pMeta->pDB->mapDb, wopt, (char *)(&uid), sizeof(uid), "", 0, &err);
      sprintf(sql, "create table st_%" PRIu64 " (uid BIGINT);", uid);
      if (sqlite3_exec(pMeta->pDB->mapDb, sql, NULL, NULL, &err) != SQLITE_OK) {
        // fprintf(stderr,"Failed to create table, since %s\n", err);
      }
      break;
    case META_CHILD_TABLE:
      // save tagDB
      rocksdb_put(pMeta->pDB->tagDb, wopt, (char *)(&uid), sizeof(uid), pTbOptions->ctbCfg.pTag,
                  kvRowLen(pTbOptions->ctbCfg.pTag), &err);

      // save mapDB
      sprintf(sql, "insert into st_%" PRIu64 " values (%" PRIu64 ");", pTbOptions->ctbCfg.suid, uid);
      if (sqlite3_exec(pMeta->pDB->mapDb, sql, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "failed to insert data, since %s\n", err);
      }
      break;
    default:
      ASSERT(0);
  }

  rocksdb_writeoptions_destroy(wopt);
#else
  char  sql[256];
  char *err = NULL;
  int   rc;

  switch (pTbOptions->type) {
    case META_SUPER_TABLE:
      // sprintf(sql, "INSERT INTO tb VALUES (\'%s\', %" PRIu64
      //              ");"
      //              "INSERT INTO stb VALUES (%" PRIu64
      //              ", \'%s\', );"
      //              "CREATE TABLE IF NOT EXISTS stb_%" PRIu64
      //              " ("
      //              "    tb_uid INTEGER NOT NULL UNIQUE,"
      //              "    tbname VARCHAR(256),"
      //              "    tag1 INTEGER"
      //              ");"
      //              "CREATE INDEX IF NOT EXISTS stb_%" PRIu64 "_tag1_idx ON stb_1638517480 (tag1);");
      rc = sqlite3_exec(pMeta->pDB, sql, NULL, NULL, &err);
      if (rc != SQLITE_OK) {
        printf("failed to create normal table since %s\n", err);
      }
      break;
    case META_NORMAL_TABLE:
      // sprintf(sql, "INSERT INTO tb VALUES (\'%s\', %" PRIu64
      //              ");"
      //              "INSERT INTO ntb VALUES (%" PRIu64 ", \'%s\', );");
      rc = sqlite3_exec(pMeta->pDB, sql, NULL, NULL, &err);
      if (rc != SQLITE_OK) {
        printf("failed to create normal table since %s\n", err);
      }
      break;
    case META_CHILD_TABLE:
      // sprintf(sql, "INSERT INTO tb VALUES (\'%s\', %" PRIu64
      //              ");"
      //              "INSERT INTO stb_%" PRIu64 " VALUES (%" PRIu64 ", \'%s\', );");
      rc = sqlite3_exec(pMeta->pDB, sql, NULL, NULL, &err);
      if (rc != SQLITE_OK) {
        printf("failed to create normal table since %s\n", err);
      }
      break;
    default:
      break;
  }

  tfree(err);
#endif

  return 0;
}

int metaRemoveTableFromDb(SMeta *pMeta, tb_uid_t uid) {
  /* TODO */
  return 0;
}

/* ------------------------ STATIC METHODS ------------------------ */
#if !USE_SQLITE_IMPL
static void metaSaveSchemaDB(SMeta *pMeta, tb_uid_t uid, STSchema *pSchema) {
  char   key[64];
  char   pBuf[1024];
  char * ppBuf = pBuf;
  size_t vsize;
  char * err = NULL;

  rocksdb_writeoptions_t *wopt = rocksdb_writeoptions_create();
  rocksdb_writeoptions_disable_WAL(wopt, 1);

  metaGetSchemaDBKey(key, uid, schemaVersion(pSchema));
  vsize = tdEncodeSchema((void **)(&ppBuf), pSchema);
  rocksdb_put(pMeta->pDB->schemaDb, wopt, key, SCHEMA_KEY_LEN, pBuf, vsize, &err);

  rocksdb_writeoptions_destroy(wopt);
}

static void metaGetSchemaDBKey(char *key, tb_uid_t uid, int sversion) {
  *(tb_uid_t *)key = uid;
  *(int *)POINTER_SHIFT(key, sizeof(tb_uid_t)) = sversion;
}
#endif