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

int metaOpenDB(SMeta *pMeta) {
  char  dir[128];
  int   rc;
  char *err = NULL;

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

  return 0;
}

void metaCloseDB(SMeta *pMeta) {
  if (pMeta->pDB) {
    sqlite3_exec(pMeta->pDB, "BEGIN;", NULL, NULL, NULL);
    sqlite3_close(pMeta->pDB);
    pMeta->pDB = NULL;
  }

  // TODO
}

int metaSaveTableToDB(SMeta *pMeta, const STbCfg *pTbOptions) {
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
        printf("failed to create child table since %s\n", err);
      }
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