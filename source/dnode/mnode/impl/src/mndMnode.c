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
#include "mndTrans.h"

#define SDB_MNODE_VER 1

static SSdbRaw *mndMnodeActionEncode(SMnodeObj *pMnodeObj) {
  SSdbRaw *pRaw = sdbAllocRaw(SDB_MNODE, SDB_MNODE_VER, sizeof(SMnodeObj));
  if (pRaw == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pMnodeObj->id);
  SDB_SET_INT64(pRaw, dataPos, pMnodeObj->createdTime)
  SDB_SET_INT64(pRaw, dataPos, pMnodeObj->updateTime)

  return pRaw;
}

static SSdbRow *mndMnodeActionDecode(SSdbRaw *pRaw) {
  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) return NULL;

  if (sver != SDB_MNODE_VER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("failed to decode mnode since %s", terrstr());
    return NULL;
  }

  SSdbRow   *pRow = sdbAllocRow(sizeof(SMnodeObj));
  SMnodeObj *pMnodeObj = sdbGetRowObj(pRow);
  if (pMnodeObj == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, pRow, dataPos, &pMnodeObj->id)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pMnodeObj->createdTime)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pMnodeObj->updateTime)

  return pRow;
}

static void mnodeResetMnode(SMnodeObj *pMnodeObj) {
  pMnodeObj->role = TAOS_SYNC_STATE_FOLLOWER;
  pMnodeObj->roleTerm = 0;
  pMnodeObj->roleTime = 0;
}

static int32_t mndMnodeActionInsert(SSdb *pSdb, SMnodeObj *pMnodeObj) {
  mTrace("mnode:%d, perform insert action", pMnodeObj->id);
  pMnodeObj->pDnode = sdbAcquire(pSdb, SDB_DNODE, &pMnodeObj->id);
  if (pMnodeObj->pDnode == NULL) {
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    return -1;
  }

  mnodeResetMnode(pMnodeObj);
  return 0;
}

static int32_t mndMnodeActionDelete(SSdb *pSdb, SMnodeObj *pMnodeObj) {
  mTrace("mnode:%d, perform delete action", pMnodeObj->id);
  if (pMnodeObj->pDnode != NULL) {
    sdbRelease(pSdb, pMnodeObj->pDnode);
    pMnodeObj->pDnode = NULL;
  }

  return 0;
}

static int32_t mndMnodeActionUpdate(SSdb *pSdb, SMnodeObj *pSrcMnode, SMnodeObj *pDstMnode) {
  mTrace("mnode:%d, perform update action", pSrcMnode->id);
  pSrcMnode->id = pDstMnode->id;
  pSrcMnode->createdTime = pDstMnode->createdTime;
  pSrcMnode->updateTime = pDstMnode->updateTime;
  mnodeResetMnode(pSrcMnode);
}

static int32_t mndCreateDefaultMnode(SMnode *pMnode) {
  SMnodeObj mnodeObj = {0};
  mnodeObj.id = 0;
  mnodeObj.createdTime = taosGetTimestampMs();
  mnodeObj.updateTime = mnodeObj.createdTime;

  SSdbRaw *pRaw = mndMnodeActionEncode(&mnodeObj);
  if (pRaw == NULL) return -1;
  sdbSetRawStatus(pRaw, SDB_STATUS_READY);

  mTrace("mnode:%d, will be created while deploy sdb", mnodeObj.id);
  return sdbWrite(pMnode->pSdb, pRaw);
}

static int32_t mndProcessCreateMnodeMsg(SMnode *pMnode, SMnodeMsg *pMsg) { return 0; }

static int32_t mndProcessDropMnodeMsg(SMnode *pMnode, SMnodeMsg *pMsg) { return 0; }

int32_t mndInitMnode(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_MNODE,
                     .keyType = SDB_KEY_INT32,
                     .deployFp = (SdbDeployFp)mndCreateDefaultMnode,
                     .encodeFp = (SdbEncodeFp)mndMnodeActionEncode,
                     .decodeFp = (SdbDecodeFp)mndMnodeActionDecode,
                     .insertFp = (SdbInsertFp)mndMnodeActionInsert,
                     .updateFp = (SdbUpdateFp)mndMnodeActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndMnodeActionDelete};

  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_CREATE_MNODE, mndProcessCreateMnodeMsg);
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_DROP_MNODE, mndProcessDropMnodeMsg);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupMnode(SMnode *pMnode) {}

bool mndIsMnode(SMnode *pMnode, int32_t dnodeId) {
  SSdb *pSdb = pMnode->pSdb;

  SMnodeObj *pMnodeObj = sdbAcquire(pSdb, SDB_MNODE, &dnodeId);
  if (pMnodeObj == NULL) {
    return false;
  }

  sdbRelease(pSdb, pMnodeObj);
  return true;
}