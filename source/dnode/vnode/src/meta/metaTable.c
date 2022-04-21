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

#include "vnodeInt.h"

int metaCreateSTable(SMeta *pMeta, SVCreateStbReq *pReq, SVCreateStbRsp *pRsp) {
  STbDbKey    tbDbKey = {0};
  SSkmDbKey   skmDbKey = {0};
  SMetaEntry  me = {0};
  int         kLen;
  int         vLen;
  const void *pKey;
  const void *pVal;

  // check name and uid unique

  // set structs
  me.type = TSDB_SUPER_TABLE;
  me.uid = pReq->suid;
  me.name = pReq->name;
  me.stbEntry.nCols = pReq->nCols;
  me.stbEntry.sver = pReq->sver;
  me.stbEntry.pSchema = pReq->pSchema;
  me.stbEntry.nTags = pReq->nTags;
  me.stbEntry.pSchemaTg = pReq->pSchemaTg;

  tbDbKey.uid = pReq->suid;
  tbDbKey.ver = 0;  // (TODO)

  skmDbKey.uid = pReq->suid;
  skmDbKey.sver = 0;  // (TODO)

  // save to table.db (TODO)
  pKey = &tbDbKey;
  kLen = sizeof(tbDbKey);
  pVal = NULL;
  vLen = 0;
  if (tdbDbInsert(pMeta->pTbDb, pKey, kLen, pVal, vLen, NULL) < 0) {
    return -1;
  }

  // save to schema.db
  pKey = &skmDbKey;
  kLen = sizeof(skmDbKey);
  pVal = NULL;
  vLen = 0;
  if (tdbDbInsert(pMeta->pSkmDb, pKey, kLen, pVal, vLen, NULL) < 0) {
    return -1;
  }

  // update name.idx
  pKey = pReq->name;
  kLen = strlen(pReq->name) + 1;
  pVal = &pReq->suid;
  vLen = sizeof(tb_uid_t);
  if (tdbDbInsert(pMeta->pNameIdx, pKey, kLen, pVal, vLen, NULL) < 0) {
    return -1;
  }

  return 0;
}

int metaCreateTable(SMeta *pMeta, STbCfg *pTbCfg) {
#if 0
  if (metaSaveTableToDB(pMeta, pTbCfg) < 0) {
    // TODO: handle error
    return -1;
  }

  if (metaSaveTableToIdx(pMeta, pTbCfg) < 0) {
    // TODO: handle error
    return -1;
  }
#endif

  return 0;
}

int metaDropTable(SMeta *pMeta, tb_uid_t uid) {
#if 0
  if (metaRemoveTableFromIdx(pMeta, uid) < 0) {
    // TODO: handle error
    return -1;
  }

  if (metaRemoveTableFromIdx(pMeta, uid) < 0) {
    // TODO
    return -1;
  }
#endif

  return 0;
}
