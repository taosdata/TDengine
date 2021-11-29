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

#define _DEFAULT_SOURCE
#include "mndInt.h"

#define SDB_ACCT_VER 1

static SSdbRaw *mnodeAcctActionEncode(SAcctObj *pAcct) {
  SSdbRaw *pRaw = sdbAllocRaw(SDB_ACCT, SDB_ACCT_VER, sizeof(SAcctObj));
  if (pRaw == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pAcct->acct, TSDB_USER_LEN)
  SDB_SET_INT64(pRaw, dataPos, pAcct->createdTime)
  SDB_SET_INT64(pRaw, dataPos, pAcct->updateTime)
  SDB_SET_INT32(pRaw, dataPos, pAcct->acctId)
  SDB_SET_INT32(pRaw, dataPos, pAcct->status)
  SDB_SET_INT32(pRaw, dataPos, pAcct->cfg.maxUsers)
  SDB_SET_INT32(pRaw, dataPos, pAcct->cfg.maxDbs)
  SDB_SET_INT32(pRaw, dataPos, pAcct->cfg.maxTimeSeries)
  SDB_SET_INT32(pRaw, dataPos, pAcct->cfg.maxStreams)
  SDB_SET_INT64(pRaw, dataPos, pAcct->cfg.maxStorage)
  SDB_SET_INT32(pRaw, dataPos, pAcct->cfg.accessState)
  SDB_SET_DATALEN(pRaw, dataPos);

  return pRaw;
}

static SSdbRow *mnodeAcctActionDecode(SSdbRaw *pRaw) {
  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) return NULL;

  if (sver != SDB_ACCT_VER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    return NULL;
  }

  SSdbRow *pRow = sdbAllocRow(sizeof(SAcctObj));
  SAcctObj *pAcct = sdbGetRowObj(pRow);
  if (pAcct == NULL) return NULL;
  
  int32_t dataPos = 0;
  SDB_GET_BINARY(pRaw, pRow, dataPos, pAcct->acct, TSDB_USER_LEN)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pAcct->createdTime)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pAcct->updateTime)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pAcct->acctId)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pAcct->status)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pAcct->cfg.maxUsers)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pAcct->cfg.maxDbs)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pAcct->cfg.maxTimeSeries)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pAcct->cfg.maxStreams)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pAcct->cfg.maxStorage)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pAcct->cfg.accessState)

  return pRow;
}

static int32_t mnodeAcctActionInsert(SAcctObj *pAcct) { return 0; }

static int32_t mnodeAcctActionDelete(SAcctObj *pAcct) { return 0; }

static int32_t mnodeAcctActionUpdate(SAcctObj *pSrcAcct, SAcctObj *pDstAcct) {
  SAcctObj tObj;
  int32_t  len = (int32_t)((int8_t *)&tObj.info - (int8_t *)&tObj);
  memcpy(pDstAcct, pSrcAcct, len);
  return 0;
}

static int32_t mnodeCreateDefaultAcct() {
  int32_t code = 0;

  SAcctObj acctObj = {0};
  tstrncpy(acctObj.acct, TSDB_DEFAULT_USER, TSDB_USER_LEN);
  acctObj.createdTime = taosGetTimestampMs();
  acctObj.updateTime = acctObj.createdTime;
  acctObj.acctId = 1;
  acctObj.cfg = (SAcctCfg){.maxUsers = 1024,
                           .maxDbs = 1024,
                           .maxTimeSeries = INT32_MAX,
                           .maxStreams = 8092,
                           .maxStorage = INT64_MAX,
                           .accessState = TSDB_VN_ALL_ACCCESS};

  SSdbRaw *pRaw = mnodeAcctActionEncode(&acctObj);
  if (pRaw == NULL) return -1;
  sdbSetRawStatus(pRaw, SDB_STATUS_READY);

  return sdbWrite(pRaw);
}

int32_t mndInitAcct(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_ACCT,
                     .keyType = SDB_KEY_BINARY,
                     .deployFp = (SdbDeployFp)mnodeCreateDefaultAcct,
                     .encodeFp = (SdbEncodeFp)mnodeAcctActionEncode,
                     .decodeFp = (SdbDecodeFp)mnodeAcctActionDecode,
                     .insertFp = (SdbInsertFp)mnodeAcctActionInsert,
                     .updateFp = (SdbUpdateFp)mnodeAcctActionUpdate,
                     .deleteFp = (SdbDeleteFp)mnodeAcctActionDelete};

  sdbSetTable(pMnode->pSdb, table);
  return 0;
}

void mndCleanupAcct(SMnode *pMnode) {}
