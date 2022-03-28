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
  // TODO
  ASSERT(0);
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
