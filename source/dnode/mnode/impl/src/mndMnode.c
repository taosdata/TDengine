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
#include "mndMnode.h"
#include "mndAuth.h"
#include "mndDnode.h"
#include "mndShow.h"
#include "mndTrans.h"
#include "mndUser.h"

#define MNODE_VER_NUMBER   1
#define MNODE_RESERVE_SIZE 64

static int32_t  mndCreateDefaultMnode(SMnode *pMnode);
static SSdbRaw *mndMnodeActionEncode(SMnodeObj *pObj);
static SSdbRow *mndMnodeActionDecode(SSdbRaw *pRaw);
static int32_t  mndMnodeActionInsert(SSdb *pSdb, SMnodeObj *pObj);
static int32_t  mndMnodeActionDelete(SSdb *pSdb, SMnodeObj *pObj);
static int32_t  mndMnodeActionUpdate(SSdb *pSdb, SMnodeObj *pOld, SMnodeObj *pNew);
static int32_t  mndProcessCreateMnodeReq(SRpcMsg *pReq);
static int32_t  mndProcessAlterMnodeReq(SRpcMsg *pReq);
static int32_t  mndProcessDropMnodeReq(SRpcMsg *pReq);
static int32_t  mndProcessCreateMnodeRsp(SRpcMsg *pRsp);
static int32_t  mndProcessAlterMnodeRsp(SRpcMsg *pRsp);
static int32_t  mndProcessDropMnodeRsp(SRpcMsg *pRsp);
static int32_t  mndRetrieveMnodes(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextMnode(SMnode *pMnode, void *pIter);

int32_t mndInitMnode(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_MNODE,
      .keyType = SDB_KEY_INT32,
      .deployFp = (SdbDeployFp)mndCreateDefaultMnode,
      .encodeFp = (SdbEncodeFp)mndMnodeActionEncode,
      .decodeFp = (SdbDecodeFp)mndMnodeActionDecode,
      .insertFp = (SdbInsertFp)mndMnodeActionInsert,
      .updateFp = (SdbUpdateFp)mndMnodeActionUpdate,
      .deleteFp = (SdbDeleteFp)mndMnodeActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_MNODE, mndProcessCreateMnodeReq);
  mndSetMsgHandle(pMnode, TDMT_DND_ALTER_MNODE, mndProcessAlterMnodeReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_MNODE, mndProcessDropMnodeReq);
  mndSetMsgHandle(pMnode, TDMT_DND_CREATE_MNODE_RSP, mndProcessCreateMnodeRsp);
  mndSetMsgHandle(pMnode, TDMT_DND_ALTER_MNODE_RSP, mndProcessAlterMnodeRsp);
  mndSetMsgHandle(pMnode, TDMT_DND_DROP_MNODE_RSP, mndProcessDropMnodeRsp);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_MNODE, mndRetrieveMnodes);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_MNODE, mndCancelGetNextMnode);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupMnode(SMnode *pMnode) {}

SMnodeObj *mndAcquireMnode(SMnode *pMnode, int32_t mnodeId) {
  SMnodeObj *pObj = sdbAcquire(pMnode->pSdb, SDB_MNODE, &mnodeId);
  if (pObj == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_MND_MNODE_NOT_EXIST;
  }
  return pObj;
}

void mndReleaseMnode(SMnode *pMnode, SMnodeObj *pObj) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pMnode->pSdb, pObj);
}

static int32_t mndCreateDefaultMnode(SMnode *pMnode) {
  SMnodeObj mnodeObj = {0};
  mnodeObj.id = 1;
  mnodeObj.createdTime = taosGetTimestampMs();
  mnodeObj.updateTime = mnodeObj.createdTime;

  SSdbRaw *pRaw = mndMnodeActionEncode(&mnodeObj);
  if (pRaw == NULL) return -1;
  sdbSetRawStatus(pRaw, SDB_STATUS_READY);

  mDebug("mnode:%d, will be created while deploy sdb, raw:%p", mnodeObj.id, pRaw);

#if 0
  return sdbWrite(pMnode->pSdb, pRaw);
#else
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_TYPE_CREATE_DNODE, NULL);
  if (pTrans == NULL) {
    mError("mnode:%d, failed to create since %s", mnodeObj.id, terrstr());
    return -1;
  }
  mDebug("trans:%d, used to create mnode:%d", pTrans->id, mnodeObj.id);

  if (mndTransAppendCommitlog(pTrans, pRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  return 0;
#endif
}

static SSdbRaw *mndMnodeActionEncode(SMnodeObj *pObj) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_MNODE, MNODE_VER_NUMBER, sizeof(SMnodeObj) + MNODE_RESERVE_SIZE);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pObj->id, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->createdTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->updateTime, _OVER)
  SDB_SET_RESERVE(pRaw, dataPos, MNODE_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("mnode:%d, failed to encode to raw:%p since %s", pObj->id, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("mnode:%d, encode to raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRaw;
}

static SSdbRow *mndMnodeActionDecode(SSdbRaw *pRaw) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) return NULL;

  if (sver != MNODE_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  SSdbRow *pRow = sdbAllocRow(sizeof(SMnodeObj));
  if (pRow == NULL) goto _OVER;

  SMnodeObj *pObj = sdbGetRowObj(pRow);
  if (pObj == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &pObj->id, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->createdTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->updateTime, _OVER)
  SDB_GET_RESERVE(pRaw, dataPos, MNODE_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("mnode:%d, failed to decode from raw:%p since %s", pObj->id, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("mnode:%d, decode from raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRow;
}

static int32_t mndMnodeActionInsert(SSdb *pSdb, SMnodeObj *pObj) {
  mTrace("mnode:%d, perform insert action, row:%p", pObj->id, pObj);
  pObj->pDnode = sdbAcquire(pSdb, SDB_DNODE, &pObj->id);
  if (pObj->pDnode == NULL) {
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    mError("mnode:%d, failed to perform insert action since %s", pObj->id, terrstr());
    return -1;
  }

  pObj->state = TAOS_SYNC_STATE_ERROR;
  return 0;
}

static int32_t mndMnodeActionDelete(SSdb *pSdb, SMnodeObj *pObj) {
  mTrace("mnode:%d, perform delete action, row:%p", pObj->id, pObj);
  if (pObj->pDnode != NULL) {
    sdbRelease(pSdb, pObj->pDnode);
    pObj->pDnode = NULL;
  }

  return 0;
}

static int32_t mndMnodeActionUpdate(SSdb *pSdb, SMnodeObj *pOld, SMnodeObj *pNew) {
  mTrace("mnode:%d, perform update action, old row:%p new row:%p", pOld->id, pOld, pNew);
  pOld->updateTime = pNew->updateTime;
  return 0;
}

bool mndIsMnode(SMnode *pMnode, int32_t dnodeId) {
  SSdb *pSdb = pMnode->pSdb;

  SMnodeObj *pObj = sdbAcquire(pSdb, SDB_MNODE, &dnodeId);
  if (pObj == NULL) {
    return false;
  }

  sdbRelease(pSdb, pObj);
  return true;
}

void mndGetMnodeEpSet(SMnode *pMnode, SEpSet *pEpSet) {
  SSdb *pSdb = pMnode->pSdb;
  pEpSet->numOfEps = 0;

  void *pIter = NULL;
  while (1) {
    SMnodeObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_MNODE, pIter, (void **)&pObj);
    if (pIter == NULL) break;
    if (pObj->pDnode == NULL) {
      mError("mnode:%d, no corresponding dnode exists", pObj->id);
    } else {
      if (pObj->state == TAOS_SYNC_STATE_LEADER) {
        pEpSet->inUse = pEpSet->numOfEps;
      }
      addEpIntoEpSet(pEpSet, pObj->pDnode->fqdn, pObj->pDnode->port);
      sdbRelease(pSdb, pObj);
    }
  }
}

static int32_t mndSetCreateMnodeRedoLogs(SMnode *pMnode, STrans *pTrans, SMnodeObj *pObj) {
  SSdbRaw *pRedoRaw = mndMnodeActionEncode(pObj);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING) != 0) return -1;
  return 0;
}

static int32_t mndSetCreateMnodeUndoLogs(SMnode *pMnode, STrans *pTrans, SMnodeObj *pObj) {
  SSdbRaw *pUndoRaw = mndMnodeActionEncode(pObj);
  if (pUndoRaw == NULL) return -1;
  if (mndTransAppendUndolog(pTrans, pUndoRaw) != 0) return -1;
  if (sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED) != 0) return -1;
  return 0;
}

static int32_t mndSetCreateMnodeCommitLogs(SMnode *pMnode, STrans *pTrans, SMnodeObj *pObj) {
  SSdbRaw *pCommitRaw = mndMnodeActionEncode(pObj);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY) != 0) return -1;
  return 0;
}

static int32_t mndSetCreateMnodeRedoActions(SMnode *pMnode, STrans *pTrans, SDnodeObj *pDnode, SMnodeObj *pObj) {
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
  int32_t numOfReplicas = 0;

  SDCreateMnodeReq createReq = {0};
  while (1) {
    SMnodeObj *pMObj = NULL;
    pIter = sdbFetch(pSdb, SDB_MNODE, pIter, (void **)&pMObj);
    if (pIter == NULL) break;

    SReplica *pReplica = &createReq.replicas[numOfReplicas];
    pReplica->id = pMObj->id;
    pReplica->port = pMObj->pDnode->port;
    memcpy(pReplica->fqdn, pMObj->pDnode->fqdn, TSDB_FQDN_LEN);
    numOfReplicas++;

    sdbRelease(pSdb, pMObj);
  }

  SReplica *pReplica = &createReq.replicas[numOfReplicas];
  pReplica->id = pDnode->id;
  pReplica->port = pDnode->port;
  memcpy(pReplica->fqdn, pDnode->fqdn, TSDB_FQDN_LEN);
  numOfReplicas++;

  createReq.replica = numOfReplicas;

  while (1) {
    SMnodeObj *pMObj = NULL;
    pIter = sdbFetch(pSdb, SDB_MNODE, pIter, (void **)&pMObj);
    if (pIter == NULL) break;

    STransAction action = {0};

    createReq.dnodeId = pMObj->id;
    int32_t contLen = tSerializeSDCreateMnodeReq(NULL, 0, &createReq);
    void   *pReq = taosMemoryMalloc(contLen);
    tSerializeSDCreateMnodeReq(pReq, contLen, &createReq);

    action.epSet = mndGetDnodeEpset(pMObj->pDnode);
    action.pCont = pReq;
    action.contLen = contLen;
    action.msgType = TDMT_DND_ALTER_MNODE;
    action.acceptableCode = TSDB_CODE_NODE_ALREADY_DEPLOYED;

    if (mndTransAppendRedoAction(pTrans, &action) != 0) {
      taosMemoryFree(pReq);
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pMObj);
      return -1;
    }

    sdbRelease(pSdb, pMObj);
  }

  {
    STransAction action = {0};
    action.epSet = mndGetDnodeEpset(pDnode);

    createReq.dnodeId = pObj->id;
    int32_t contLen = tSerializeSDCreateMnodeReq(NULL, 0, &createReq);
    void   *pReq = taosMemoryMalloc(contLen);
    tSerializeSDCreateMnodeReq(pReq, contLen, &createReq);

    action.epSet = mndGetDnodeEpset(pDnode);
    action.pCont = pReq;
    action.contLen = contLen;
    action.msgType = TDMT_DND_CREATE_MNODE;
    action.acceptableCode = TSDB_CODE_NODE_ALREADY_DEPLOYED;
    if (mndTransAppendRedoAction(pTrans, &action) != 0) {
      taosMemoryFree(pReq);
      return -1;
    }
  }

  return 0;
}

static int32_t mndCreateMnode(SMnode *pMnode, SRpcMsg *pReq, SDnodeObj *pDnode, SMCreateMnodeReq *pCreate) {
  int32_t code = -1;

  SMnodeObj mnodeObj = {0};
  mnodeObj.id = pDnode->id;
  mnodeObj.createdTime = taosGetTimestampMs();
  mnodeObj.updateTime = mnodeObj.createdTime;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_TYPE_CREATE_MNODE, pReq);
  if (pTrans == NULL) goto _OVER;

  mDebug("trans:%d, used to create mnode:%d", pTrans->id, pCreate->dnodeId);
  if (mndSetCreateMnodeRedoLogs(pMnode, pTrans, &mnodeObj) != 0) goto _OVER;
  if (mndSetCreateMnodeCommitLogs(pMnode, pTrans, &mnodeObj) != 0) goto _OVER;
  if (mndSetCreateMnodeRedoActions(pMnode, pTrans, pDnode, &mnodeObj) != 0) goto _OVER;

  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessCreateMnodeReq(SRpcMsg *pReq) {
  SMnode          *pMnode = pReq->info.node;
  int32_t          code = -1;
  SMnodeObj       *pObj = NULL;
  SDnodeObj       *pDnode = NULL;
  SUserObj        *pUser = NULL;
  SMCreateMnodeReq createReq = {0};

  if (tDeserializeSCreateDropMQSBNodeReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mDebug("mnode:%d, start to create", createReq.dnodeId);

  pObj = mndAcquireMnode(pMnode, createReq.dnodeId);
  if (pObj != NULL) {
    terrno = TSDB_CODE_MND_MNODE_ALREADY_EXIST;
    goto _OVER;
  } else if (terrno != TSDB_CODE_MND_MNODE_NOT_EXIST) {
    goto _OVER;
  }

  pDnode = mndAcquireDnode(pMnode, createReq.dnodeId);
  if (pDnode == NULL) {
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    goto _OVER;
  }

  pUser = mndAcquireUser(pMnode, pReq->conn.user);
  if (pUser == NULL) {
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
    goto _OVER;
  }

  if (mndCheckNodeAuth(pUser)) {
    goto _OVER;
  }

  code = mndCreateMnode(pMnode, pReq, pDnode, &createReq);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("mnode:%d, failed to create since %s", createReq.dnodeId, terrstr());
  }

  mndReleaseMnode(pMnode, pObj);
  mndReleaseDnode(pMnode, pDnode);
  mndReleaseUser(pMnode, pUser);

  return code;
}

static int32_t mndSetDropMnodeRedoLogs(SMnode *pMnode, STrans *pTrans, SMnodeObj *pObj) {
  SSdbRaw *pRedoRaw = mndMnodeActionEncode(pObj);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING) != 0) return -1;
  return 0;
}

static int32_t mndSetDropMnodeCommitLogs(SMnode *pMnode, STrans *pTrans, SMnodeObj *pObj) {
  SSdbRaw *pCommitRaw = mndMnodeActionEncode(pObj);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED) != 0) return -1;
  return 0;
}

static int32_t mndSetDropMnodeRedoActions(SMnode *pMnode, STrans *pTrans, SDnodeObj *pDnode, SMnodeObj *pObj) {
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
  int32_t numOfReplicas = 0;

  SDAlterMnodeReq alterReq = {0};
  while (1) {
    SMnodeObj *pMObj = NULL;
    pIter = sdbFetch(pSdb, SDB_MNODE, pIter, (void **)&pMObj);
    if (pIter == NULL) break;

    if (pMObj->id != pObj->id) {
      SReplica *pReplica = &alterReq.replicas[numOfReplicas];
      pReplica->id = pMObj->id;
      pReplica->port = pMObj->pDnode->port;
      memcpy(pReplica->fqdn, pMObj->pDnode->fqdn, TSDB_FQDN_LEN);
      numOfReplicas++;
    }

    sdbRelease(pSdb, pMObj);
  }

  alterReq.replica = numOfReplicas;

  while (1) {
    SMnodeObj *pMObj = NULL;
    pIter = sdbFetch(pSdb, SDB_MNODE, pIter, (void **)&pMObj);
    if (pIter == NULL) break;
    if (pMObj->id != pObj->id) {
      STransAction action = {0};

      alterReq.dnodeId = pMObj->id;
      int32_t contLen = tSerializeSDCreateMnodeReq(NULL, 0, &alterReq);
      void   *pReq = taosMemoryMalloc(contLen);
      tSerializeSDCreateMnodeReq(pReq, contLen, &alterReq);

      action.epSet = mndGetDnodeEpset(pMObj->pDnode);
      action.pCont = pReq;
      action.contLen = contLen;
      action.msgType = TDMT_DND_ALTER_MNODE;
      action.acceptableCode = TSDB_CODE_NODE_ALREADY_DEPLOYED;

      if (mndTransAppendRedoAction(pTrans, &action) != 0) {
        taosMemoryFree(pReq);
        sdbCancelFetch(pSdb, pIter);
        sdbRelease(pSdb, pMObj);
        return -1;
      }
    }

    sdbRelease(pSdb, pMObj);
  }

  {
    STransAction action = {0};
    action.epSet = mndGetDnodeEpset(pDnode);

    SDDropMnodeReq dropReq = {0};
    dropReq.dnodeId = pObj->id;
    int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &dropReq);
    void   *pReq = taosMemoryMalloc(contLen);
    tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &dropReq);

    action.epSet = mndGetDnodeEpset(pDnode);
    action.pCont = pReq;
    action.contLen = contLen;
    action.msgType = TDMT_DND_DROP_MNODE;
    action.acceptableCode = TSDB_CODE_NODE_NOT_DEPLOYED;
    if (mndTransAppendRedoAction(pTrans, &action) != 0) {
      taosMemoryFree(pReq);
      return -1;
    }
  }

  return 0;
}

static int32_t mndDropMnode(SMnode *pMnode, SRpcMsg *pReq, SMnodeObj *pObj) {
  int32_t code = -1;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_TYPE_DROP_MNODE, pReq);
  if (pTrans == NULL) goto _OVER;

  mDebug("trans:%d, used to drop mnode:%d", pTrans->id, pObj->id);

  if (mndSetDropMnodeRedoLogs(pMnode, pTrans, pObj) != 0) goto _OVER;
  if (mndSetDropMnodeCommitLogs(pMnode, pTrans, pObj) != 0) goto _OVER;
  if (mndSetDropMnodeRedoActions(pMnode, pTrans, pObj->pDnode, pObj) != 0) goto _OVER;
  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessDropMnodeReq(SRpcMsg *pReq) {
  SMnode        *pMnode = pReq->info.node;
  int32_t        code = -1;
  SUserObj      *pUser = NULL;
  SMnodeObj     *pObj = NULL;
  SMDropMnodeReq dropReq = {0};

  if (tDeserializeSCreateDropMQSBNodeReq(pReq->pCont, pReq->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mDebug("mnode:%d, start to drop", dropReq.dnodeId);

  if (dropReq.dnodeId <= 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  pObj = mndAcquireMnode(pMnode, dropReq.dnodeId);
  if (pObj == NULL) {
    goto _OVER;
  }

  if (pMnode->selfDnodeId == dropReq.dnodeId) {
    terrno = TSDB_CODE_MND_CANT_DROP_MASTER;
    goto _OVER;
  }

  if (sdbGetSize(pMnode->pSdb, SDB_MNODE) <= 1) {
    terrno = TSDB_CODE_MND_TOO_FEW_MNODES;
    goto _OVER;
  }

  pUser = mndAcquireUser(pMnode, pReq->conn.user);
  if (pUser == NULL) {
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
    goto _OVER;
  }

  if (mndCheckNodeAuth(pUser)) {
    goto _OVER;
  }

  code = mndDropMnode(pMnode, pReq, pObj);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("mnode:%d, failed to drop since %s", dropReq.dnodeId, terrstr());
  }

  mndReleaseMnode(pMnode, pObj);
  mndReleaseUser(pMnode, pUser);

  return code;
}

static int32_t mndProcessCreateMnodeRsp(SRpcMsg *pRsp) {
  mndTransProcessRsp(pRsp);
  return 0;
}

static int32_t mndProcessAlterMnodeRsp(SRpcMsg *pRsp) {
  mndTransProcessRsp(pRsp);
  return 0;
}

static int32_t mndProcessDropMnodeRsp(SRpcMsg *pRsp) {
  mndTransProcessRsp(pRsp);
  return 0;
}

static int32_t mndRetrieveMnodes(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode    *pMnode = pReq->info.node;
  SSdb      *pSdb = pMnode->pSdb;
  int32_t    numOfRows = 0;
  int32_t    cols = 0;
  SMnodeObj *pObj = NULL;
  char      *pWrite;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_MNODE, pShow->pIter, (void **)&pObj);
    if (pShow->pIter == NULL) break;

    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)&pObj->id, false);

    char b1[TSDB_EP_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(b1, pObj->pDnode->ep, pShow->pMeta->pSchemas[cols].bytes);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, b1, false);

    const char *roles = NULL;
    if (pObj->id == pMnode->selfDnodeId) {
      roles = syncStr(TAOS_SYNC_STATE_LEADER);
    } else {
      roles = syncStr(pObj->state);
    }
    char *b2 = taosMemoryCalloc(1, 12 + VARSTR_HEADER_SIZE);
    STR_WITH_MAXSIZE_TO_VARSTR(b2, roles, pShow->pMeta->pSchemas[cols].bytes);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)b2, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)&pObj->createdTime, false);

    numOfRows++;
    sdbRelease(pSdb, pObj);
  }

  pShow->numOfRows += numOfRows;

  return numOfRows;
}

static void mndCancelGetNextMnode(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}

static int32_t mndProcessAlterMnodeReq(SRpcMsg *pReq) {
  SMnode          *pMnode = pReq->info.node;
  SDAlterMnodeReq alterReq = {0};

  if (tDeserializeSDCreateMnodeReq(pReq->pCont, pReq->contLen, &alterReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (alterReq.dnodeId != pMnode->selfDnodeId) {
    terrno = TSDB_CODE_INVALID_OPTION;
    mError("failed to alter mnode since %s, input:%d cur:%d", terrstr(), alterReq.dnodeId, pMnode->selfDnodeId);
    return -1;
  }

  SSyncCfg cfg = {.replicaNum = alterReq.replica, .myIndex = -1};
  for (int32_t i = 0; i < alterReq.replica; ++i) {
    SNodeInfo *pNode = &cfg.nodeInfo[i];
    tstrncpy(pNode->nodeFqdn, alterReq.replicas[i].fqdn, sizeof(pNode->nodeFqdn));
    pNode->nodePort = alterReq.replicas[i].port;
    if (alterReq.replicas[i].id == pMnode->selfDnodeId) cfg.myIndex = i;
  }

  if (cfg.myIndex == -1) {
    mError("failed to alter mnode since myindex is -1");
    return -1;
  } else {
    mInfo("start to alter mnode sync, replica:%d myindex:%d", cfg.replicaNum, cfg.myIndex);
    for (int32_t i = 0; i < alterReq.replica; ++i) {
      SNodeInfo *pNode = &cfg.nodeInfo[i];
      mInfo("index:%d, fqdn:%s port:%d", i, pNode->nodeFqdn, pNode->nodePort);
    }
  }

  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  pMgmt->standby = 0;
  int32_t code = syncReconfig(pMgmt->sync, &cfg);
  if (code != 0) {
    mError("failed to alter mnode sync since %s", terrstr());
    return code;
  } else {
    pMgmt->errCode = 0;
    tsem_wait(&pMgmt->syncSem);
    mInfo("alter mnode sync result:%s", tstrerror(pMgmt->errCode));
    terrno = pMgmt->errCode;
    return pMgmt->errCode;
  }
}
