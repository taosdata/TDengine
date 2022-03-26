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
#include "mndAcct.h"
#include "mndShow.h"

#define TSDB_ACCT_VER_NUMBER 1
#define TSDB_ACCT_RESERVE_SIZE 128

static int32_t  mndCreateDefaultAcct(SMnode *pMnode);
static SSdbRaw *mndAcctActionEncode(SAcctObj *pAcct);
static SSdbRow *mndAcctActionDecode(SSdbRaw *pRaw);
static int32_t  mndAcctActionInsert(SSdb *pSdb, SAcctObj *pAcct);
static int32_t  mndAcctActionDelete(SSdb *pSdb, SAcctObj *pAcct);
static int32_t  mndAcctActionUpdate(SSdb *pSdb, SAcctObj *pOld, SAcctObj *pNew);
static int32_t  mndProcessCreateAcctReq(SNodeMsg *pReq);
static int32_t  mndProcessAlterAcctReq(SNodeMsg *pReq);
static int32_t  mndProcessDropAcctReq(SNodeMsg *pReq);

int32_t mndInitAcct(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_ACCT,
                     .keyType = SDB_KEY_BINARY,
                     .deployFp = mndCreateDefaultAcct,
                     .encodeFp = (SdbEncodeFp)mndAcctActionEncode,
                     .decodeFp = (SdbDecodeFp)mndAcctActionDecode,
                     .insertFp = (SdbInsertFp)mndAcctActionInsert,
                     .updateFp = (SdbUpdateFp)mndAcctActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndAcctActionDelete};

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_ACCT, mndProcessCreateAcctReq);
  mndSetMsgHandle(pMnode, TDMT_MND_ALTER_ACCT, mndProcessAlterAcctReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_ACCT, mndProcessDropAcctReq);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupAcct(SMnode *pMnode) {}

static int32_t mndCreateDefaultAcct(SMnode *pMnode) {
  SAcctObj acctObj = {0};
  tstrncpy(acctObj.acct, TSDB_DEFAULT_USER, TSDB_USER_LEN);
  acctObj.createdTime = taosGetTimestampMs();
  acctObj.updateTime = acctObj.createdTime;
  acctObj.acctId = 1;
  acctObj.cfg = (SAcctCfg){.maxUsers = INT32_MAX,
                           .maxDbs = INT32_MAX,
                           .maxStbs = INT32_MAX,
                           .maxTbs = INT32_MAX,
                           .maxTimeSeries = INT32_MAX,
                           .maxStreams = INT32_MAX,
                           .maxFuncs = INT32_MAX,
                           .maxConsumers = INT32_MAX,
                           .maxConns = INT32_MAX,
                           .maxTopics = INT32_MAX,
                           .maxStorage = INT64_MAX,
                           .accessState = TSDB_VN_ALL_ACCCESS};

  SSdbRaw *pRaw = mndAcctActionEncode(&acctObj);
  if (pRaw == NULL) return -1;
  sdbSetRawStatus(pRaw, SDB_STATUS_READY);

  mDebug("acct:%s, will be created while deploy sdb, raw:%p", acctObj.acct, pRaw);
  return sdbWrite(pMnode->pSdb, pRaw);
}

static SSdbRaw *mndAcctActionEncode(SAcctObj *pAcct) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_ACCT, TSDB_ACCT_VER_NUMBER, sizeof(SAcctObj) + TSDB_ACCT_RESERVE_SIZE);
  if (pRaw == NULL) goto ACCT_ENCODE_OVER;

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pAcct->acct, TSDB_USER_LEN, ACCT_ENCODE_OVER)
  SDB_SET_INT64(pRaw, dataPos, pAcct->createdTime, ACCT_ENCODE_OVER)
  SDB_SET_INT64(pRaw, dataPos, pAcct->updateTime, ACCT_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pAcct->acctId, ACCT_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pAcct->status, ACCT_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pAcct->cfg.maxUsers, ACCT_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pAcct->cfg.maxDbs, ACCT_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pAcct->cfg.maxStbs, ACCT_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pAcct->cfg.maxTbs, ACCT_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pAcct->cfg.maxTimeSeries, ACCT_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pAcct->cfg.maxStreams, ACCT_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pAcct->cfg.maxFuncs, ACCT_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pAcct->cfg.maxConsumers, ACCT_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pAcct->cfg.maxConns, ACCT_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pAcct->cfg.maxTopics, ACCT_ENCODE_OVER)
  SDB_SET_INT64(pRaw, dataPos, pAcct->cfg.maxStorage, ACCT_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pAcct->cfg.accessState, ACCT_ENCODE_OVER)
  SDB_SET_RESERVE(pRaw, dataPos, TSDB_ACCT_RESERVE_SIZE, ACCT_ENCODE_OVER)
  SDB_SET_DATALEN(pRaw, dataPos, ACCT_ENCODE_OVER)

  terrno = 0;

ACCT_ENCODE_OVER:
  if (terrno != 0) {
    mError("acct:%s, failed to encode to raw:%p since %s", pAcct->acct, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("acct:%s, encode to raw:%p, row:%p", pAcct->acct, pRaw, pAcct);
  return pRaw;
}

static SSdbRow *mndAcctActionDecode(SSdbRaw *pRaw) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto ACCT_DECODE_OVER;

  if (sver != TSDB_ACCT_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto ACCT_DECODE_OVER;
  }

  SSdbRow *pRow = sdbAllocRow(sizeof(SAcctObj));
  if (pRow == NULL) goto ACCT_DECODE_OVER;

  SAcctObj *pAcct = sdbGetRowObj(pRow);
  if (pAcct == NULL) goto ACCT_DECODE_OVER;

  int32_t dataPos = 0;
  SDB_GET_BINARY(pRaw, dataPos, pAcct->acct, TSDB_USER_LEN, ACCT_DECODE_OVER)
  SDB_GET_INT64(pRaw, dataPos, &pAcct->createdTime, ACCT_DECODE_OVER)
  SDB_GET_INT64(pRaw, dataPos, &pAcct->updateTime, ACCT_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pAcct->acctId, ACCT_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pAcct->status, ACCT_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pAcct->cfg.maxUsers, ACCT_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pAcct->cfg.maxDbs, ACCT_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pAcct->cfg.maxStbs, ACCT_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pAcct->cfg.maxTbs, ACCT_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pAcct->cfg.maxTimeSeries, ACCT_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pAcct->cfg.maxStreams, ACCT_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pAcct->cfg.maxFuncs, ACCT_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pAcct->cfg.maxConsumers, ACCT_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pAcct->cfg.maxConns, ACCT_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pAcct->cfg.maxTopics, ACCT_DECODE_OVER)
  SDB_GET_INT64(pRaw, dataPos, &pAcct->cfg.maxStorage, ACCT_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pAcct->cfg.accessState, ACCT_DECODE_OVER)
  SDB_GET_RESERVE(pRaw, dataPos, TSDB_ACCT_RESERVE_SIZE, ACCT_DECODE_OVER)

  terrno = 0;

ACCT_DECODE_OVER:
  if (terrno != 0) {
    mError("acct:%s, failed to decode from raw:%p since %s", pAcct->acct, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("acct:%s, decode from raw:%p, row:%p", pAcct->acct, pRaw, pAcct);
  return pRow;
}

static int32_t mndAcctActionInsert(SSdb *pSdb, SAcctObj *pAcct) {
  mTrace("acct:%s, perform insert action, row:%p", pAcct->acct, pAcct);
  return 0;
}

static int32_t mndAcctActionDelete(SSdb *pSdb, SAcctObj *pAcct) {
  mTrace("acct:%s, perform delete action, row:%p", pAcct->acct, pAcct);
  return 0;
}

static int32_t mndAcctActionUpdate(SSdb *pSdb, SAcctObj *pOld, SAcctObj *pNew) {
  mTrace("acct:%s, perform update action, old row:%p new row:%p", pOld->acct, pOld, pNew);

  pOld->updateTime = pNew->updateTime;
  pOld->status = pNew->status;
  memcpy(&pOld->cfg, &pNew->cfg, sizeof(SAcctCfg));
  return 0;
}

static int32_t mndProcessCreateAcctReq(SNodeMsg *pReq) {
  terrno = TSDB_CODE_MND_MSG_NOT_PROCESSED;
  mError("failed to process create acct request since %s", terrstr());
  return -1;
}

static int32_t mndProcessAlterAcctReq(SNodeMsg *pReq) {
  terrno = TSDB_CODE_MND_MSG_NOT_PROCESSED;
  mError("failed to process create acct request since %s", terrstr());
  return -1;
}

static int32_t mndProcessDropAcctReq(SNodeMsg *pReq) {
  terrno = TSDB_CODE_MND_MSG_NOT_PROCESSED;
  mError("failed to process create acct request since %s", terrstr());
  return -1;
}