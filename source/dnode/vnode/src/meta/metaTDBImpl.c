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

#include "meta.h"

#include "tdb.h"

struct SMetaDB {
  // TODO
};

int metaOpenDB(SMeta *pMeta) {
  // TODO
  return 0;
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