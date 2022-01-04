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
#define TSDB_ACCT_RESERVE_SIZE 64

static int32_t  mndCreateDefaultAcct(SMnode *pMnode);
static SSdbRaw *mndAcctActionEncode(SAcctObj *pAcct);
static SSdbRow *mndAcctActionDecode(SSdbRaw *pRaw);
static int32_t  mndAcctActionInsert(SSdb *pSdb, SAcctObj *pAcct);
static int32_t  mndAcctActionDelete(SSdb *pSdb, SAcctObj *pAcct);
static int32_t  mndAcctActionUpdate(SSdb *pSdb, SAcctObj *pOldAcct, SAcctObj *pNewAcct);
static int32_t  mndProcessCreateAcctMsg(SMnodeMsg *pMnodeMsg);
static int32_t  mndProcessAlterAcctMsg(SMnodeMsg *pMnodeMsg);
static int32_t  mndProcessDropAcctMsg(SMnodeMsg *pMnodeMsg);

int32_t mndInitAcct(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_ACCT,
                     .keyType = SDB_KEY_BINARY,
                     .deployFp = mndCreateDefaultAcct,
                     .encodeFp = (SdbEncodeFp)mndAcctActionEncode,
                     .decodeFp = (SdbDecodeFp)mndAcctActionDecode,
                     .insertFp = (SdbInsertFp)mndAcctActionInsert,
                     .updateFp = (SdbUpdateFp)mndAcctActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndAcctActionDelete};

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_ACCT, mndProcessCreateAcctMsg);
  mndSetMsgHandle(pMnode, TDMT_MND_ALTER_ACCT, mndProcessAlterAcctMsg);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_ACCT, mndProcessDropAcctMsg);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupAcct(SMnode *pMnode) {}

static int32_t mndCreateDefaultAcct(SMnode *pMnode) {
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

  SSdbRaw *pRaw = mndAcctActionEncode(&acctObj);
  if (pRaw == NULL) return -1;
  sdbSetRawStatus(pRaw, SDB_STATUS_READY);

  mDebug("acct:%s, will be created while deploy sdb", acctObj.acct);
  return sdbWrite(pMnode->pSdb, pRaw);
}

static SSdbRaw *mndAcctActionEncode(SAcctObj *pAcct) {
  SSdbRaw *pRaw = sdbAllocRaw(SDB_ACCT, TSDB_ACCT_VER_NUMBER, sizeof(SAcctObj) + TSDB_ACCT_RESERVE_SIZE);
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
  SDB_SET_RESERVE(pRaw, dataPos, TSDB_ACCT_RESERVE_SIZE)
  SDB_SET_DATALEN(pRaw, dataPos);

  return pRaw;
}

static SSdbRow *mndAcctActionDecode(SSdbRaw *pRaw) {
  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) return NULL;

  if (sver != TSDB_ACCT_VER_NUMBER) {
    mError("failed to decode acct since %s", terrstr());
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    return NULL;
  }

  SSdbRow  *pRow = sdbAllocRow(sizeof(SAcctObj));
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
  SDB_GET_RESERVE(pRaw, pRow, dataPos, TSDB_ACCT_RESERVE_SIZE)

  return pRow;
}

static int32_t mndAcctActionInsert(SSdb *pSdb, SAcctObj *pAcct) {
  mTrace("acct:%s, perform insert action", pAcct->acct);
  return 0;
}

static int32_t mndAcctActionDelete(SSdb *pSdb, SAcctObj *pAcct) {
  mTrace("acct:%s, perform delete action", pAcct->acct);
  return 0;
}

static int32_t mndAcctActionUpdate(SSdb *pSdb, SAcctObj *pOldAcct, SAcctObj *pNewAcct) {
  mTrace("acct:%s, perform update action", pOldAcct->acct);

  pOldAcct->updateTime = pNewAcct->updateTime;
  pOldAcct->status = pNewAcct->status;
  memcpy(&pOldAcct->cfg, &pNewAcct->cfg, sizeof(SAcctInfo));
  return 0;
}

static int32_t mndProcessCreateAcctMsg(SMnodeMsg *pMnodeMsg) {
  terrno = TSDB_CODE_MND_MSG_NOT_PROCESSED;
  mError("failed to process create acct msg since %s", terrstr());
  return -1;
}

static int32_t mndProcessAlterAcctMsg(SMnodeMsg *pMnodeMsg) {
  terrno = TSDB_CODE_MND_MSG_NOT_PROCESSED;
  mError("failed to process create acct msg since %s", terrstr());
  return -1;
}

static int32_t mndProcessDropAcctMsg(SMnodeMsg *pMnodeMsg) {
  terrno = TSDB_CODE_MND_MSG_NOT_PROCESSED;
  mError("failed to process create acct msg since %s", terrstr());
  return -1;
}