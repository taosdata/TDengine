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
#include "sqlite3.h"

struct SMetaDB {
  sqlite3 *pDB;
};

int metaOpenDB(SMeta *pMeta) {
  char  dir[128];
  int   rc;
  char *err = NULL;

  pMeta->pDB = (SMetaDB *)calloc(1, sizeof(SMetaDB));
  if (pMeta->pDB == NULL) {
    // TODO: handle error
    return -1;
  }

  sprintf(dir, "%s/meta.db", pMeta->path);
  rc = sqlite3_open(dir, &(pMeta->pDB->pDB));
  if (rc != SQLITE_OK) {
    // TODO: handle error
    printf("failed to open meta.db\n");
  }

  // For all tables
  rc = sqlite3_exec(pMeta->pDB->pDB,
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
  rc = sqlite3_exec(pMeta->pDB->pDB,
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
  rc = sqlite3_exec(pMeta->pDB->pDB,
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

  sqlite3_exec(pMeta->pDB->pDB, "BEGIN;", NULL, NULL, &err);

  tfree(err);

  return 0;
}

void metaCloseDB(SMeta *pMeta) {
  if (pMeta->pDB) {
    sqlite3_exec(pMeta->pDB->pDB, "COMMIT;", NULL, NULL, NULL);
    sqlite3_close(pMeta->pDB->pDB);
    free(pMeta->pDB);
    pMeta->pDB = NULL;
  }

  // TODO
}

int metaSaveTableToDB(SMeta *pMeta, const STbCfg *pTbCfg) {
  char          sql[256];
  char *        err = NULL;
  int           rc;
  tb_uid_t      uid;
  sqlite3_stmt *stmt;
  char          buf[256];
  void *        pBuf;

  switch (pTbCfg->type) {
    case META_SUPER_TABLE:
      uid = pTbCfg->stbCfg.suid;
      sprintf(sql,
              "INSERT INTO tb VALUES (\'%s\', %" PRIu64
              ");"
              "CREATE TABLE IF NOT EXISTS stb_%" PRIu64
              " ("
              "    tb_uid INTEGER NOT NULL UNIQUE,"
              "    tbname VARCHAR(256),"
              "    tag1 INTEGER);",
              pTbCfg->name, uid, uid);
      rc = sqlite3_exec(pMeta->pDB->pDB, sql, NULL, NULL, &err);
      if (rc != SQLITE_OK) {
        printf("failed to create normal table since %s\n", err);
      }

      sprintf(sql, "INSERT INTO stb VALUES (%" PRIu64 ", %s, ?, ?)", uid, pTbCfg->name);
      sqlite3_prepare_v2(pMeta->pDB->pDB, sql, -1, &stmt, NULL);

      pBuf = buf;
      tdEncodeSchema(&pBuf, pTbCfg->stbCfg.pSchema);
      sqlite3_bind_blob(stmt, 1, buf, POINTER_DISTANCE(pBuf, buf), NULL);
      pBuf = buf;
      tdEncodeSchema(&pBuf, pTbCfg->stbCfg.pTagSchema);
      sqlite3_bind_blob(stmt, 2, buf, POINTER_DISTANCE(pBuf, buf), NULL);

      sqlite3_step(stmt);

      sqlite3_finalize(stmt);

#if 0
      sprintf(sql,
              "INSERT INTO tb VALUES (?, ?);"
              // "INSERT INTO stb VALUES (?, ?, ?, ?);"
              // "CREATE TABLE IF NOT EXISTS stb_%" PRIu64
              // " ("
              // "    tb_uid INTEGER NOT NULL UNIQUE,"
              // "    tbname VARCHAR(256),"
              // "    tag1 INTEGER);"
              ,
              uid);
      rc = sqlite3_prepare_v2(pMeta->pDB->pDB, sql, -1, &stmt, NULL);
      if (rc != SQLITE_OK) {
        return -1;
      }
      sqlite3_bind_text(stmt, 1, pTbCfg->name, -1, SQLITE_TRANSIENT);
      sqlite3_bind_int64(stmt, 2, uid);
      sqlite3_step(stmt);
      sqlite3_finalize(stmt);


      // sqlite3_bind_int64(stmt, 3, uid);
      // sqlite3_bind_text(stmt, 4, pTbCfg->name, -1, SQLITE_TRANSIENT);
      // pBuf = buf;
      // tdEncodeSchema(&pBuf, pTbCfg->stbCfg.pSchema);
      // sqlite3_bind_blob(stmt, 5, buf, POINTER_DISTANCE(pBuf, buf), NULL);
      // pBuf = buf;
      // tdEncodeSchema(&pBuf, pTbCfg->stbCfg.pTagSchema);
      // sqlite3_bind_blob(stmt, 6, buf, POINTER_DISTANCE(pBuf, buf), NULL);

      rc = sqliteVjj3_step(stmt);
      if (rc != SQLITE_OK) {
        printf("failed to create normal table since %s\n", sqlite3_errmsg(pMeta->pDB->pDB));
      }
      sqlite3_finalize(stmt);
#endif
      break;
    case META_NORMAL_TABLE:
      // uid = metaGenerateUid(pMeta);
      // sprintf(sql,
      //         "INSERT INTO tb VALUES (\'%s\', %" PRIu64
      //         ");"
      //         "INSERT INTO ntb VALUES (%" PRIu64 ", \'%s\', );",
      //         pTbCfg->name, uid, uid, pTbCfg->name, );

      // rc = sqlite3_exec(pMeta->pDB->pDB, sql, NULL, NULL, &err);
      // if (rc != SQLITE_OK) {
      //   printf("failed to create normal table since %s\n", err);
      // }
      break;
    case META_CHILD_TABLE:
#if 0
      uid = metaGenerateUid(pMeta);
      // sprintf(sql, "INSERT INTO tb VALUES (\'%s\', %" PRIu64
      //              ");"
      //              "INSERT INTO stb_%" PRIu64 " VALUES (%" PRIu64 ", \'%s\', );");
      rc = sqlite3_exec(pMeta->pDB->pDB, sql, NULL, NULL, &err);
      if (rc != SQLITE_OK) {
        printf("failed to create child table since %s\n", err);
      }
#endif
      break;
    default:
      break;
  }

  tfree(err);

  return 0;
}

int metaRemoveTableFromDb(SMeta *pMeta, tb_uid_t uid) {
  /* TODO */
  return 0;
}