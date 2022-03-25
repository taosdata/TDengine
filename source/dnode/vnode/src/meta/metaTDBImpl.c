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

#include "tdb.h"

struct SMetaDB {
  TENV *pEnv;
  TDB * pTbDB;
  TDB * pSchemaDB;
  TDB * pNameIdx;
  TDB * pStbIdx;
  TDB * pNtbIdx;
  TDB * pCtbIdx;
  // tag index hash table
  // suid+colid --> TDB *
  struct {
  } tagIdxHt;
};

#define A(op, flag)                 \
  do {                              \
    if ((ret = op) != 0) goto flag; \
  } while (0)

int metaOpenDB(SMeta *pMeta) {
  SMetaDB *pDb;
  TENV *   pEnv;
  TDB *    pTbDB;
  TDB *    pSchemaDB;
  TDB *    pNameIdx;
  TDB *    pStbIdx;
  TDB *    pNtbIdx;
  TDB *    pCtbIdx;
  int      ret;

  pDb = (SMetaDB *)taosMemoryCalloc(1, sizeof(*pDb));
  if (pDb == NULL) {
    return -1;
  }

  // Create and open the ENV
  A((tdbEnvCreate(&pEnv)), _err);
#if 0
  // Set options of the environment
  A(tdbEnvSetPageSize(pEnv, 8192), _err);
  A(tdbEnvSetCacheSize(pEnv, 16 * 1024 * 1024), _err);
#endif
  A((tdbEnvOpen(&pEnv)), _err);

  // Create and open each DB
  A(tdbCreate(&pTbDB), _err);
  A(tdbOpen(&pTbDB, "table.db", NULL, pEnv), _err);

  A(tdbCreate(&pSchemaDB), _err);
  A(tdbOpen(&pSchemaDB, "schema.db", NULL, pEnv), _err);

  A(tdbCreate(&pNameIdx), _err);
  A(tdbOpen(&pNameIdx, "name.db", NULL, pEnv), _err);
  // tdbAssociate();

  pDb->pEnv = pEnv;
  pDb->pTbDB = pTbDB;
  pDb->pSchemaDB = pSchemaDB;
  pMeta->pDB = pDb;
  return 0;

_err:
  return -1;
}

void metaCloseDB(SMeta *pMeta) {
  // TODO
}

int metaSaveTableToDB(SMeta *pMeta, STbCfg *pTbCfg) {
  // TODO
  return 0;
}

int metaRemoveTableFromDb(SMeta *pMeta, tb_uid_t uid) {
  // TODO
  return 0;
}

STbCfg *metaGetTbInfoByUid(SMeta *pMeta, tb_uid_t uid) {
  // TODO
  return NULL;
}

STbCfg *metaGetTbInfoByName(SMeta *pMeta, char *tbname, tb_uid_t *uid) {
  // TODO
  return NULL;
}

SSchemaWrapper *metaGetTableSchema(SMeta *pMeta, tb_uid_t uid, int32_t sver, bool isinline) {
  // TODO
  return NULL;
}

STSchema *metaGetTbTSchema(SMeta *pMeta, tb_uid_t uid, int32_t sver) {
  // TODO
  return NULL;
}

SMTbCursor *metaOpenTbCursor(SMeta *pMeta) {
  // TODO
  return NULL;
}

void metaCloseTbCursor(SMTbCursor *pTbCur) {
  // TODO
}

char *metaTbCursorNext(SMTbCursor *pTbCur) {
  // TODO
  return NULL;
}

SMCtbCursor *metaOpenCtbCursor(SMeta *pMeta, tb_uid_t uid) {
  // TODO
  return NULL;
}

void metaCloseCtbCurosr(SMCtbCursor *pCtbCur) {
  // TODO
}

tb_uid_t metaCtbCursorNext(SMCtbCursor *pCtbCur) {
  // TODO
  return 0;
}