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

#include "tdbInt.h"

struct SMetaDB {
  TENV *pEnv;
  TDB  *pTbDB;
  TDB  *pSchemaDB;
  TDB  *pNameIdx;
  TDB  *pStbIdx;
  TDB  *pNtbIdx;
  TDB  *pCtbIdx;
};

int metaOpenDB(SMeta *pMeta) {
  SMetaDB *pMetaDb;
  int      ret;

  // allocate DB handle
  pMetaDb = taosMemoryCalloc(1, sizeof(*pMetaDb));
  if (pMetaDb == NULL) {
    // TODO
    ASSERT(0);
    return -1;
  }

  // open the ENV
  ret = tdbEnvOpen(pMeta->path, 4096, 256, &(pMetaDb->pEnv));
  if (ret < 0) {
    // TODO
    ASSERT(0);
    return -1;
  }

  // open table DB
  ret = tdbDbOpen("table.db", sizeof(tb_uid_t), TDB_VARIANT_LEN, NULL /*TODO*/, pMetaDb->pEnv, &(pMetaDb->pTbDB));
  if (ret < 0) {
    // TODO
    ASSERT(0);
    return -1;
  }

  // open schema DB
  ret = tdbDbOpen("schema.db", sizeof(tb_uid_t) + sizeof(int32_t), TDB_VARIANT_LEN, NULL /*TODO*/, pMetaDb->pEnv,
                  &(pMetaDb->pSchemaDB));
  if (ret < 0) {
    // TODO
    ASSERT(0);
    return -1;
  }

  ret = tdbDbOpen("name.idx", TDB_VARIANT_LEN, 0, NULL /*TODO*/, pMetaDb->pEnv, &(pMetaDb->pNameIdx));
  if (ret < 0) {
    // TODO
    ASSERT(0);
    return -1;
  }

  ret = tdbDbOpen("stb.idx", sizeof(tb_uid_t), 0, NULL /*TODO*/, pMetaDb->pEnv, &(pMetaDb->pStbIdx));
  if (ret < 0) {
    // TODO
    ASSERT(0);
    return -1;
  }

  ret = tdbDbOpen("ntb.idx", sizeof(tb_uid_t), 0, NULL /*TODO*/, pMetaDb->pEnv, &(pMetaDb->pNtbIdx));
  if (ret < 0) {
    // TODO
    ASSERT(0);
    return -1;
  }

  ret = tdbDbOpen("ctb.idx", sizeof(tb_uid_t), 0, NULL /*TODO*/, pMetaDb->pEnv, &(pMetaDb->pCtbIdx));
  if (ret < 0) {
    // TODO
    ASSERT(0);
    return -1;
  }

  return 0;
}

void metaCloseDB(SMeta *pMeta) {
  // TODO
  ASSERT(0);
}

int metaSaveTableToDB(SMeta *pMeta, STbCfg *pTbCfg) {
  // TODO
  return 0;
}

int metaRemoveTableFromDb(SMeta *pMeta, tb_uid_t uid) {
  // TODO
  ASSERT(0);
  return 0;
}

STbCfg *metaGetTbInfoByUid(SMeta *pMeta, tb_uid_t uid) {
  // TODO
  ASSERT(0);
  return NULL;
}

STbCfg *metaGetTbInfoByName(SMeta *pMeta, char *tbname, tb_uid_t *uid) {
  // TODO
  ASSERT(0);
  return NULL;
}

SSchemaWrapper *metaGetTableSchema(SMeta *pMeta, tb_uid_t uid, int32_t sver, bool isinline) {
  // TODO
  ASSERT(0);
  return NULL;
}

STSchema *metaGetTbTSchema(SMeta *pMeta, tb_uid_t uid, int32_t sver) {
  // TODO
  ASSERT(0);
  return NULL;
}

SMTbCursor *metaOpenTbCursor(SMeta *pMeta) {
  // TODO
  ASSERT(0);
  return NULL;
}

void metaCloseTbCursor(SMTbCursor *pTbCur) {
  // TODO
  ASSERT(0);
}

char *metaTbCursorNext(SMTbCursor *pTbCur) {
  // TODO
  ASSERT(0);
  return NULL;
}

SMCtbCursor *metaOpenCtbCursor(SMeta *pMeta, tb_uid_t uid) {
  // TODO
  ASSERT(0);
  return NULL;
}

void metaCloseCtbCurosr(SMCtbCursor *pCtbCur) {
  // TODO
  ASSERT(0);
}

tb_uid_t metaCtbCursorNext(SMCtbCursor *pCtbCur) {
  // TODO
  ASSERT(0);
  return 0;
}

int metaGetTbNum(SMeta *pMeta) {
  // TODO
  ASSERT(0);
  return 0;
}

STSmaWrapper *metaGetSmaInfoByTable(SMeta *pMeta, tb_uid_t uid) {
  // TODO
  ASSERT(0);
  return NULL;
}

int metaRemoveSmaFromDb(SMeta *pMeta, int64_t indexUid) {
  // TODO
  ASSERT(0);
  return 0;
}

int metaSaveSmaToDB(SMeta *pMeta, STSma *pSmaCfg) {
  // TODO
  ASSERT(0);
  return 0;
}

STSma *metaGetSmaInfoByIndex(SMeta *pMeta, int64_t indexUid) {
  // TODO
  ASSERT(0);
  return NULL;
}

const char *metaSmaCursorNext(SMSmaCursor *pCur) {
  // TODO
  ASSERT(0);
  return NULL;
}

void metaCloseSmaCurosr(SMSmaCursor *pCur) {
  // TODO
  ASSERT(0);
}

SArray *metaGetSmaTbUids(SMeta *pMeta, bool isDup) {
  // TODO
  ASSERT(0);
  return NULL;
}

SMSmaCursor *metaOpenSmaCursor(SMeta *pMeta, tb_uid_t uid) {
  // TODO
  ASSERT(0);
  return NULL;
}
