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

#define TSDB_MNODE_VER_NUMBER   1
#define TSDB_MNODE_RESERVE_SIZE 64

static int32_t  mndCreateDefaultMnode(SMnode *pMnode);
static SSdbRaw *mndMnodeActionEncode(SMnodeObj *pObj);
static SSdbRow *mndMnodeActionDecode(SSdbRaw *pRaw);
static int32_t  mndMnodeActionInsert(SSdb *pSdb, SMnodeObj *pObj);
static int32_t  mndMnodeActionDelete(SSdb *pSdb, SMnodeObj *pObj);
static int32_t  mndMnodeActionUpdate(SSdb *pSdb, SMnodeObj *pOld, SMnodeObj *pNew);
static int32_t  mndProcessCreateMnodeReq(SNodeMsg *pReq);
static int32_t  mndProcessDropMnodeReq(SNodeMsg *pReq);
static int32_t  mndProcessCreateMnodeRsp(SNodeMsg *pRsp);
static int32_t  mndProcessAlterMnodeRsp(SNodeMsg *pRsp);
static int32_t  mndProcessDropMnodeRsp(SNodeMsg *pRsp);
static int32_t  mndGetMnodeMeta(SNodeMsg *pReq, SShowObj *pShow, STableMetaRsp *pMeta);
static int32_t  mndRetrieveMnodes(SNodeMsg *pReq, SShowObj *pShow, char *data, int32_t rows);
static void     mndCancelGetNextMnode(SMnode *pMnode, void *pIter);

int32_t mndInitMnode(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_MNODE,
                     .keyType = SDB_KEY_INT32,
                     .deployFp = (SdbDeployFp)mndCreateDefaultMnode,
                     .encodeFp = (SdbEncodeFp)mndMnodeActionEncode,
                     .decodeFp = (SdbDecodeFp)mndMnodeActionDecode,
                     .insertFp = (SdbInsertFp)mndMnodeActionInsert,
                     .updateFp = (SdbUpdateFp)mndMnodeActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndMnodeActionDelete};

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_MNODE, mndProcessCreateMnodeReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_MNODE, mndProcessDropMnodeReq);
  mndSetMsgHandle(pMnode, TDMT_DND_CREATE_MNODE_RSP, mndProcessCreateMnodeRsp);
  mndSetMsgHandle(pMnode, TDMT_DND_ALTER_MNODE_RSP, mndProcessAlterMnodeRsp);
  mndSetMsgHandle(pMnode, TDMT_DND_DROP_MNODE_RSP, mndProcessDropMnodeRsp);

  mndAddShowMetaHandle(pMnode, TSDB_MGMT_TABLE_MNODE, mndGetMnodeMeta);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_MNODE, mndRetrieveMnodes);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_MNODE, mndCancelGetNextMnode);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupMnode(SMnode *pMnode) {}

static SMnodeObj *mndAcquireMnode(SMnode *pMnode, int32_t mnodeId) {
  SSdb      *pSdb = pMnode->pSdb;
  SMnodeObj *pObj = sdbAcquire(pSdb, SDB_MNODE, &mnodeId);
  if (pObj == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_MND_MNODE_NOT_EXIST;
  }
  return pObj;
}

static void mndReleaseMnode(SMnode *pMnode, SMnodeObj *pObj) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pObj);
}

char *mndGetRoleStr(int32_t showType) {
  switch (showType) {
    case TAOS_SYNC_STATE_FOLLOWER:
      return "unsynced";
    case TAOS_SYNC_STATE_CANDIDATE:
      return "slave";
    case TAOS_SYNC_STATE_LEADER:
      return "master";
    default:
      return "undefined";
  }
}

void mndUpdateMnodeRole(SMnode *pMnode) {
  SSdb *pSdb = pMnode->pSdb;
  void *pIter = NULL;
  while (1) {
    SMnodeObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_MNODE, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    ESyncState lastRole = pObj->role;
    if (pObj->id == 1) {
      pObj->role = TAOS_SYNC_STATE_LEADER;
    } else {
      pObj->role = TAOS_SYNC_STATE_CANDIDATE;
    }
    if (pObj->role != lastRole) {
      pObj->roleTime = taosGetTimestampMs();
    }

    sdbRelease(pSdb, pObj);
  }
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
  return sdbWrite(pMnode->pSdb, pRaw);
}

static SSdbRaw *mndMnodeActionEncode(SMnodeObj *pObj) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_MNODE, TSDB_MNODE_VER_NUMBER, sizeof(SMnodeObj) + TSDB_MNODE_RESERVE_SIZE);
  if (pRaw == NULL) goto MNODE_ENCODE_OVER;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pObj->id, MNODE_ENCODE_OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->createdTime, MNODE_ENCODE_OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->updateTime, MNODE_ENCODE_OVER)
  SDB_SET_RESERVE(pRaw, dataPos, TSDB_MNODE_RESERVE_SIZE, MNODE_ENCODE_OVER)

  terrno = 0;

MNODE_ENCODE_OVER:
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

  if (sver != TSDB_MNODE_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto MNODE_DECODE_OVER;
  }

  SSdbRow *pRow = sdbAllocRow(sizeof(SMnodeObj));
  if (pRow == NULL) goto MNODE_DECODE_OVER;

  SMnodeObj *pObj = sdbGetRowObj(pRow);
  if (pObj == NULL) goto MNODE_DECODE_OVER;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &pObj->id, MNODE_DECODE_OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->createdTime, MNODE_DECODE_OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->updateTime, MNODE_DECODE_OVER)
  SDB_GET_RESERVE(pRaw, dataPos, TSDB_MNODE_RESERVE_SIZE, MNODE_DECODE_OVER)

  terrno = 0;

MNODE_DECODE_OVER:
  if (terrno != 0) {
    mError("mnode:%d, failed to decode from raw:%p since %s", pObj->id, pRaw, terrstr());
    tfree(pRow);
    return NULL;
  }

  mTrace("mnode:%d, decode from raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRow;
}

static void mnodeResetMnode(SMnodeObj *pObj) { pObj->role = TAOS_SYNC_STATE_FOLLOWER; }

static int32_t mndMnodeActionInsert(SSdb *pSdb, SMnodeObj *pObj) {
  mTrace("mnode:%d, perform insert action, row:%p", pObj->id, pObj);
  pObj->pDnode = sdbAcquire(pSdb, SDB_DNODE, &pObj->id);
  if (pObj->pDnode == NULL) {
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    mError("mnode:%d, failed to perform insert action since %s", pObj->id, terrstr());
    return -1;
  }

  mnodeResetMnode(pObj);
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
    if (pObj->pDnode == NULL) break;

    if (pObj->role == TAOS_SYNC_STATE_LEADER) {
      pEpSet->inUse = pEpSet->numOfEps;
    }

    addEpIntoEpSet(pEpSet, pObj->pDnode->fqdn, pObj->pDnode->port);
    sdbRelease(pSdb, pObj);
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
    void   *pReq = malloc(contLen);
    tSerializeSDCreateMnodeReq(pReq, contLen, &createReq);

    action.epSet = mndGetDnodeEpset(pMObj->pDnode);
    action.pCont = pReq;
    action.contLen = contLen;
    action.msgType = TDMT_DND_ALTER_MNODE;
    action.acceptableCode = TSDB_CODE_NODE_ALREADY_DEPLOYED;

    if (mndTransAppendRedoAction(pTrans, &action) != 0) {
      free(pReq);
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
    void   *pReq = malloc(contLen);
    tSerializeSDCreateMnodeReq(pReq, contLen, &createReq);

    action.epSet = mndGetDnodeEpset(pDnode);
    action.pCont = pReq;
    action.contLen = contLen;
    action.msgType = TDMT_DND_CREATE_MNODE;
    action.acceptableCode = TSDB_CODE_NODE_ALREADY_DEPLOYED;
    if (mndTransAppendRedoAction(pTrans, &action) != 0) {
      free(pReq);
      return -1;
    }
  }

  return 0;
}

static int32_t mndCreateMnode(SMnode *pMnode, SNodeMsg *pReq, SDnodeObj *pDnode, SMCreateMnodeReq *pCreate) {
  int32_t code = -1;

  SMnodeObj mnodeObj = {0};
  mnodeObj.id = pDnode->id;
  mnodeObj.createdTime = taosGetTimestampMs();
  mnodeObj.updateTime = mnodeObj.createdTime;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_TYPE_CREATE_MNODE, &pReq->rpcMsg);
  if (pTrans == NULL) goto CREATE_MNODE_OVER;

  mDebug("trans:%d, used to create mnode:%d", pTrans->id, pCreate->dnodeId);
  if (mndSetCreateMnodeRedoLogs(pMnode, pTrans, &mnodeObj) != 0) goto CREATE_MNODE_OVER;
  if (mndSetCreateMnodeCommitLogs(pMnode, pTrans, &mnodeObj) != 0) goto CREATE_MNODE_OVER;
  if (mndSetCreateMnodeRedoActions(pMnode, pTrans, pDnode, &mnodeObj) != 0) goto CREATE_MNODE_OVER;

  if (mndTransPrepare(pMnode, pTrans) != 0) goto CREATE_MNODE_OVER;

  code = 0;

CREATE_MNODE_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessCreateMnodeReq(SNodeMsg *pReq) {
  SMnode          *pMnode = pReq->pNode;
  int32_t          code = -1;
  SMnodeObj       *pObj = NULL;
  SDnodeObj       *pDnode = NULL;
  SUserObj        *pUser = NULL;
  SMCreateMnodeReq createReq = {0};

  if (tDeserializeSMCreateDropMnodeReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto CREATE_MNODE_OVER;
  }

  mDebug("mnode:%d, start to create", createReq.dnodeId);

  pObj = mndAcquireMnode(pMnode, createReq.dnodeId);
  if (pObj != NULL) {
    terrno = TSDB_CODE_MND_MNODE_ALREADY_EXIST;
    goto CREATE_MNODE_OVER;
  } else if (terrno != TSDB_CODE_MND_MNODE_NOT_EXIST) {
    goto CREATE_MNODE_OVER;
  }

  pDnode = mndAcquireDnode(pMnode, createReq.dnodeId);
  if (pDnode == NULL) {
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    goto CREATE_MNODE_OVER;
  }

  pUser = mndAcquireUser(pMnode, pReq->user);
  if (pUser == NULL) {
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
    goto CREATE_MNODE_OVER;
  }

  if (mndCheckNodeAuth(pUser)) {
    goto CREATE_MNODE_OVER;
  }

  code = mndCreateMnode(pMnode, pReq, pDnode, &createReq);
  if (code == 0) code = TSDB_CODE_MND_ACTION_IN_PROGRESS;

CREATE_MNODE_OVER:
  if (code != 0 && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
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
      void   *pReq = malloc(contLen);
      tSerializeSDCreateMnodeReq(pReq, contLen, &alterReq);

      action.epSet = mndGetDnodeEpset(pMObj->pDnode);
      action.pCont = pReq;
      action.contLen = contLen;
      action.msgType = TDMT_DND_ALTER_MNODE;
      action.acceptableCode = TSDB_CODE_NODE_ALREADY_DEPLOYED;

      if (mndTransAppendRedoAction(pTrans, &action) != 0) {
        free(pReq);
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
    int32_t contLen = tSerializeSMCreateDropMnodeReq(NULL, 0, &dropReq);
    void   *pReq = malloc(contLen);
    tSerializeSMCreateDropMnodeReq(pReq, contLen, &dropReq);

    action.epSet = mndGetDnodeEpset(pDnode);
    action.pCont = pReq;
    action.contLen = contLen;
    action.msgType = TDMT_DND_DROP_MNODE;
    action.acceptableCode = TSDB_CODE_NODE_NOT_DEPLOYED;
    if (mndTransAppendRedoAction(pTrans, &action) != 0) {
      free(pReq);
      return -1;
    }
  }

  return 0;
}

static int32_t mndDropMnode(SMnode *pMnode, SNodeMsg *pReq, SMnodeObj *pObj) {
  int32_t code = -1;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_TYPE_DROP_MNODE, &pReq->rpcMsg);
  if (pTrans == NULL) goto DROP_MNODE_OVER;

  mDebug("trans:%d, used to drop mnode:%d", pTrans->id, pObj->id);

  if (mndSetDropMnodeRedoLogs(pMnode, pTrans, pObj) != 0) goto DROP_MNODE_OVER;
  if (mndSetDropMnodeCommitLogs(pMnode, pTrans, pObj) != 0) goto DROP_MNODE_OVER;
  if (mndSetDropMnodeRedoActions(pMnode, pTrans, pObj->pDnode, pObj) != 0) goto DROP_MNODE_OVER;
  if (mndTransPrepare(pMnode, pTrans) != 0) goto DROP_MNODE_OVER;

  code = 0;

DROP_MNODE_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessDropMnodeReq(SNodeMsg *pReq) {
  SMnode        *pMnode = pReq->pNode;
  int32_t        code = -1;
  SUserObj      *pUser = NULL;
  SMnodeObj     *pObj = NULL;
  SMDropMnodeReq dropReq = {0};

  if (tDeserializeSMCreateDropMnodeReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto DROP_MNODE_OVER;
  }

  mDebug("mnode:%d, start to drop", dropReq.dnodeId);

  if (dropReq.dnodeId <= 0) {
    terrno = TSDB_CODE_SDB_APP_ERROR;
    goto DROP_MNODE_OVER;
  }

  pObj = mndAcquireMnode(pMnode, dropReq.dnodeId);
  if (pObj == NULL) {
    goto DROP_MNODE_OVER;
  }

  pUser = mndAcquireUser(pMnode, pReq->user);
  if (pUser == NULL) {
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
    goto DROP_MNODE_OVER;
  }

  if (mndCheckNodeAuth(pUser)) {
    goto DROP_MNODE_OVER;
  }

  code = mndDropMnode(pMnode, pReq, pObj);
  if (code == 0) code = TSDB_CODE_MND_ACTION_IN_PROGRESS;

DROP_MNODE_OVER:
  if (code != 0 && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("mnode:%d, failed to drop since %s", dropReq.dnodeId, terrstr());
  }

  mndReleaseMnode(pMnode, pObj);
  mndReleaseUser(pMnode, pUser);

  return code;
}

static int32_t mndProcessCreateMnodeRsp(SNodeMsg *pRsp) {
  mndTransProcessRsp(pRsp);
  return 0;
}

static int32_t mndProcessAlterMnodeRsp(SNodeMsg *pRsp) {
  mndTransProcessRsp(pRsp);
  return 0;
}

static int32_t mndProcessDropMnodeRsp(SNodeMsg *pRsp) {
  mndTransProcessRsp(pRsp);
  return 0;
}

static int32_t mndGetMnodeMeta(SNodeMsg *pReq, SShowObj *pShow, STableMetaRsp *pMeta) {
  SMnode *pMnode = pReq->pNode;
  SSdb   *pSdb = pMnode->pSdb;

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->pSchemas;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "id");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = TSDB_EP_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "endpoint");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 12 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "role");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create_time");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "role_time");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pMeta->numOfColumns = cols;
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = sdbGetSize(pSdb, SDB_MNODE);
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  strcpy(pMeta->tbName, mndShowStr(pShow->type));

  mndUpdateMnodeRole(pMnode);
  return 0;
}

static int32_t mndRetrieveMnodes(SNodeMsg *pReq, SShowObj *pShow, char *data, int32_t rows) {
  SMnode    *pMnode = pReq->pNode;
  SSdb      *pSdb = pMnode->pSdb;
  int32_t    numOfRows = 0;
  int32_t    cols = 0;
  SMnodeObj *pObj = NULL;
  char      *pWrite;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_MNODE, pShow->pIter, (void **)&pObj);
    if (pShow->pIter == NULL) break;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pObj->id;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pObj->pDnode->ep, pShow->bytes[cols]);

    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    char *roles = mndGetRoleStr(pObj->role);
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, roles, pShow->bytes[cols]);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pObj->roleTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pObj->createdTime;
    cols++;

    numOfRows++;
    sdbRelease(pSdb, pObj);
  }

  mndVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  pShow->numOfReads += numOfRows;

  return numOfRows;
}

static void mndCancelGetNextMnode(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}
