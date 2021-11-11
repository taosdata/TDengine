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
#include "mnodeInt.h"

#define ACCT_VER 1

static SSdbRaw *mnodeAcctActionEncode(SAcctObj *pAcct) {
  SSdbRaw *pRaw = calloc(1, sizeof(SAcctObj) + sizeof(SSdbRaw));
  if (pRaw == NULL) {
    terrno = TSDB_CODE_MND_OUT_OF_MEMORY;
    return NULL;
  }

  int32_t dataLen = 0;
  char   *pData = pRaw->data;
  SDB_SET_BINARY_VAL(pData, dataLen, pAcct->acct, TSDB_USER_LEN)
  SDB_SET_INT64_VAL(pData, dataLen, pAcct->createdTime)
  SDB_SET_INT64_VAL(pData, dataLen, pAcct->updateTime)
  SDB_SET_INT32_VAL(pData, dataLen, pAcct->acctId)
  SDB_SET_INT32_VAL(pData, dataLen, pAcct->status)
  SDB_SET_INT32_VAL(pData, dataLen, pAcct->cfg.maxUsers)
  SDB_SET_INT32_VAL(pData, dataLen, pAcct->cfg.maxDbs)
  SDB_SET_INT32_VAL(pData, dataLen, pAcct->cfg.maxTimeSeries)
  SDB_SET_INT32_VAL(pData, dataLen, pAcct->cfg.maxStreams)
  SDB_SET_INT64_VAL(pData, dataLen, pAcct->cfg.maxStorage)
  SDB_SET_INT32_VAL(pData, dataLen, pAcct->cfg.accessState)

  pRaw->dataLen = dataLen;
  pRaw->type = SDB_ACCT;
  pRaw->sver = ACCT_VER;
  return pRaw;
}

static SAcctObj *mnodeAcctActionDecode(SSdbRaw *pRaw) {
  if (pRaw->sver != ACCT_VER) {
    terrno = TSDB_CODE_SDB_INVAID_RAW_DATA_VER;
    return NULL;
  }

  SAcctObj *pAcct = calloc(1, sizeof(SAcctObj));
  if (pAcct == NULL) {
    terrno = TSDB_CODE_MND_OUT_OF_MEMORY;
    return NULL;
  }

  int32_t code = 0;
  int32_t dataLen = pRaw->dataLen;
  char   *pData = pRaw->data;
  SDB_GET_BINARY_VAL(pData, dataLen, pAcct->acct, TSDB_USER_LEN, code)
  SDB_GET_INT64_VAL(pData, dataLen, pAcct->createdTime, code)
  SDB_GET_INT64_VAL(pData, dataLen, pAcct->updateTime, code)
  SDB_GET_INT32_VAL(pData, dataLen, pAcct->acctId, code)
  SDB_GET_INT32_VAL(pData, dataLen, pAcct->status, code)
  SDB_GET_INT32_VAL(pData, dataLen, pAcct->cfg.maxUsers, code)
  SDB_GET_INT32_VAL(pData, dataLen, pAcct->cfg.maxDbs, code)
  SDB_GET_INT32_VAL(pData, dataLen, pAcct->cfg.maxTimeSeries, code)
  SDB_GET_INT32_VAL(pData, dataLen, pAcct->cfg.maxStreams, code)
  SDB_GET_INT64_VAL(pData, dataLen, pAcct->cfg.maxStorage, code)
  SDB_GET_INT32_VAL(pData, dataLen, pAcct->cfg.accessState, code)

  if (code != 0) {
    tfree(pAcct);
    terrno = code;
    return NULL;
  }

  return pAcct;
}

static int32_t mnodeAcctActionInsert(SAcctObj *pAcct) { return 0; }

static int32_t mnodeAcctActionDelete(SAcctObj *pAcct) { return 0; }

static int32_t mnodeAcctActionUpdate(SAcctObj *pSrcAcct, SAcctObj *pDstAcct) {
  memcpy(pDstAcct, pSrcAcct, (int32_t)((char *)&pDstAcct->info - (char *)&pDstAcct));
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
  if (pRaw != NULL) {
    return -1;
  }

  return sdbWrite(pRaw);
}

int32_t mnodeInitAcct() {
  SSdbDesc desc = {.sdbType = SDB_ACCT,
                   .keyType = SDB_KEY_BINARY,
                   .deployFp = (SdbDeployFp)mnodeCreateDefaultAcct,
                   .encodeFp = (SdbEncodeFp)mnodeAcctActionEncode,
                   .decodeFp = (SdbDecodeFp)mnodeAcctActionDecode,
                   .insertFp = (SdbInsertFp)mnodeAcctActionInsert,
                   .updateFp = (SdbUpdateFp)mnodeAcctActionUpdate,
                   .deleteFp = (SdbDeleteFp)mnodeAcctActionDelete};
  sdbSetHandler(desc);

  return 0;
}

void mnodeCleanupAcct() {}
