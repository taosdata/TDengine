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
#include "audit.h"
#include "mndCluster.h"
#include "mndDnode.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndSync.h"
#include "mndTrans.h"
#include "tmisce.h"

#define MNODE_VER_NUMBER   2
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
static int32_t  mndRetrieveMnodes(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextMnode(SMnode *pMnode, void *pIter);
static void     mndReloadSyncConfig(SMnode *pMnode);

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
  mndSetMsgHandle(pMnode, TDMT_DND_CREATE_MNODE_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_DND_ALTER_MNODE_TYPE_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_ALTER_MNODE, mndProcessAlterMnodeReq);
  mndSetMsgHandle(pMnode, TDMT_MND_ALTER_MNODE_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_MNODE, mndProcessDropMnodeReq);
  mndSetMsgHandle(pMnode, TDMT_DND_DROP_MNODE_RSP, mndTransProcessRsp);

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
  (void)sdbSetRawStatus(pRaw, SDB_STATUS_READY);

  mInfo("mnode:%d, will be created when deploying, raw:%p", mnodeObj.id, pRaw);

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_GLOBAL, NULL, "create-mnode");
  if (pTrans == NULL) {
    sdbFreeRaw(pRaw);
    mError("mnode:%d, failed to create since %s", mnodeObj.id, terrstr());
    return -1;
  }
  mInfo("trans:%d, used to create mnode:%d", pTrans->id, mnodeObj.id);

  if (mndTransAppendCommitlog(pTrans, pRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  (void)sdbSetRawStatus(pRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  return 0;
}

static SSdbRaw *mndMnodeActionEncode(SMnodeObj *pObj) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_MNODE, MNODE_VER_NUMBER, sizeof(SMnodeObj) + MNODE_RESERVE_SIZE);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pObj->id, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->createdTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->updateTime, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->role, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->lastIndex, _OVER)
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
  SSdbRow   *pRow = NULL;
  SMnodeObj *pObj = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) return NULL;

  if (sver != 1 && sver != 2) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  pRow = sdbAllocRow(sizeof(SMnodeObj));
  if (pRow == NULL) goto _OVER;

  pObj = sdbGetRowObj(pRow);
  if (pObj == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &pObj->id, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->createdTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->updateTime, _OVER)
  if (sver >= 2) {
    SDB_GET_INT32(pRaw, dataPos, &pObj->role, _OVER)
    SDB_GET_INT64(pRaw, dataPos, &pObj->lastIndex, _OVER)
  }
  SDB_GET_RESERVE(pRaw, dataPos, MNODE_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("mnode:%d, failed to decode from raw:%p since %s", pObj == NULL ? 0 : pObj->id, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("mnode:%d, decode from raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRow;
}

static int32_t mndMnodeActionInsert(SSdb *pSdb, SMnodeObj *pObj) {
  mTrace("mnode:%d, perform insert action, row:%p", pObj->id, pObj);
  pObj->pDnode = sdbAcquireNotReadyObj(pSdb, SDB_DNODE, &pObj->id);
  if (pObj->pDnode == NULL) {
    mError("mnode:%d, failed to perform insert action since %s", pObj->id, terrstr());
    return -1;
  }

  pObj->syncState = TAOS_SYNC_STATE_OFFLINE;
  mndReloadSyncConfig(pSdb->pMnode);
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
  pOld->role = pNew->role;
  pOld->updateTime = pNew->updateTime;
  pOld->lastIndex = pNew->lastIndex;
  mndReloadSyncConfig(pSdb->pMnode);

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
  SSdb   *pSdb = pMnode->pSdb;
  int32_t totalMnodes = sdbGetSize(pSdb, SDB_MNODE);
  if (totalMnodes == 0) {
    syncGetRetryEpSet(pMnode->syncMgmt.sync, pEpSet);
    return;
  }

  void *pIter = NULL;
  while (1) {
    SMnodeObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_MNODE, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    if (pObj->id == pMnode->selfDnodeId) {
      if (mndIsLeader(pMnode)) {
        pEpSet->inUse = pEpSet->numOfEps;
      } else {
        pEpSet->inUse = (pEpSet->numOfEps + 1) % totalMnodes;
        // pEpSet->inUse = 0;
      }
    }
    if (pObj->pDnode != NULL) {
      addEpIntoEpSet(pEpSet, pObj->pDnode->fqdn, pObj->pDnode->port);
    }
    sdbRelease(pSdb, pObj);
  }

  if (pEpSet->numOfEps == 0) {
    syncGetRetryEpSet(pMnode->syncMgmt.sync, pEpSet);
  }

  if (pEpSet->inUse >= pEpSet->numOfEps) {
    pEpSet->inUse = 0;
  }
  epsetSort(pEpSet);
}

static int32_t mndSetCreateMnodeRedoLogs(SMnode *pMnode, STrans *pTrans, SMnodeObj *pObj) {
  SSdbRaw *pRedoRaw = mndMnodeActionEncode(pObj);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING) != 0) return -1;
  return 0;
}

int32_t mndSetRestoreCreateMnodeRedoLogs(SMnode *pMnode, STrans *pTrans, SMnodeObj *pObj) {
  SSdbRaw *pRedoRaw = mndMnodeActionEncode(pObj);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_READY) != 0) return -1;
  return 0;
}

static int32_t mndSetCreateMnodeUndoLogs(SMnode *pMnode, STrans *pTrans, SMnodeObj *pObj) {
  SSdbRaw *pUndoRaw = mndMnodeActionEncode(pObj);
  if (pUndoRaw == NULL) return -1;
  if (mndTransAppendUndolog(pTrans, pUndoRaw) != 0) return -1;
  if (sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED) != 0) return -1;
  return 0;
}

int32_t mndSetCreateMnodeCommitLogs(SMnode *pMnode, STrans *pTrans, SMnodeObj *pObj) {
  SSdbRaw *pCommitRaw = mndMnodeActionEncode(pObj);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY) != 0) return -1;
  return 0;
}

static int32_t mndBuildCreateMnodeRedoAction(STrans *pTrans, SDCreateMnodeReq *pCreateReq, SEpSet *pCreateEpSet) {
  int32_t contLen = tSerializeSDCreateMnodeReq(NULL, 0, pCreateReq);
  void   *pReq = taosMemoryMalloc(contLen);
  tSerializeSDCreateMnodeReq(pReq, contLen, pCreateReq);

  STransAction action = {
      .epSet = *pCreateEpSet,
      .pCont = pReq,
      .contLen = contLen,
      .msgType = TDMT_DND_CREATE_MNODE,
      .acceptableCode = TSDB_CODE_MNODE_ALREADY_DEPLOYED,
  };

  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }
  return 0;
}

static int32_t mndBuildAlterMnodeTypeRedoAction(STrans *pTrans, SDAlterMnodeTypeReq *pAlterMnodeTypeReq,
                                                SEpSet *pAlterMnodeTypeEpSet) {
  int32_t contLen = tSerializeSDCreateMnodeReq(NULL, 0, pAlterMnodeTypeReq);
  void   *pReq = taosMemoryMalloc(contLen);
  tSerializeSDCreateMnodeReq(pReq, contLen, pAlterMnodeTypeReq);

  STransAction action = {
      .epSet = *pAlterMnodeTypeEpSet,
      .pCont = pReq,
      .contLen = contLen,
      .msgType = TDMT_DND_ALTER_MNODE_TYPE,
      .retryCode = TSDB_CODE_MNODE_NOT_CATCH_UP,
      .acceptableCode = TSDB_CODE_MNODE_ALREADY_IS_VOTER,
  };

  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }
  return 0;
}

static int32_t mndBuildAlterMnodeRedoAction(STrans *pTrans, SDCreateMnodeReq *pAlterReq, SEpSet *pAlterEpSet) {
  int32_t contLen = tSerializeSDCreateMnodeReq(NULL, 0, pAlterReq);
  void   *pReq = taosMemoryMalloc(contLen);
  tSerializeSDCreateMnodeReq(pReq, contLen, pAlterReq);

  STransAction action = {
      .epSet = *pAlterEpSet,
      .pCont = pReq,
      .contLen = contLen,
      .msgType = TDMT_MND_ALTER_MNODE,
      .acceptableCode = 0,
  };

  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }

  return 0;
}

static int32_t mndBuildDropMnodeRedoAction(STrans *pTrans, SDDropMnodeReq *pDropReq, SEpSet *pDroprEpSet) {
  int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, pDropReq);
  void   *pReq = taosMemoryMalloc(contLen);
  tSerializeSCreateDropMQSNodeReq(pReq, contLen, pDropReq);

  STransAction action = {
      .epSet = *pDroprEpSet,
      .pCont = pReq,
      .contLen = contLen,
      .msgType = TDMT_DND_DROP_MNODE,
      .acceptableCode = TSDB_CODE_MNODE_NOT_DEPLOYED,
  };

  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }
  return 0;
}

static int32_t mndSetCreateMnodeRedoActions(SMnode *pMnode, STrans *pTrans, SDnodeObj *pDnode, SMnodeObj *pObj) {
  SSdb            *pSdb = pMnode->pSdb;
  void            *pIter = NULL;
  int32_t          numOfReplicas = 0;
  int32_t          numOfLearnerReplicas = 0;
  SDCreateMnodeReq createReq = {0};
  SEpSet           createEpset = {0};

  while (1) {
    SMnodeObj *pMObj = NULL;
    pIter = sdbFetch(pSdb, SDB_MNODE, pIter, (void **)&pMObj);
    if (pIter == NULL) break;

    if (pMObj->role == TAOS_SYNC_ROLE_VOTER) {
      createReq.replicas[numOfReplicas].id = pMObj->id;
      createReq.replicas[numOfReplicas].port = pMObj->pDnode->port;
      memcpy(createReq.replicas[numOfReplicas].fqdn, pMObj->pDnode->fqdn, TSDB_FQDN_LEN);
      numOfReplicas++;
    } else {
      createReq.learnerReplicas[numOfLearnerReplicas].id = pMObj->id;
      createReq.learnerReplicas[numOfLearnerReplicas].port = pMObj->pDnode->port;
      memcpy(createReq.learnerReplicas[numOfLearnerReplicas].fqdn, pMObj->pDnode->fqdn, TSDB_FQDN_LEN);
      numOfLearnerReplicas++;
    }

    sdbRelease(pSdb, pMObj);
  }

  createReq.replica = numOfReplicas;
  createReq.learnerReplica = numOfLearnerReplicas + 1;
  createReq.learnerReplicas[numOfLearnerReplicas].id = pDnode->id;
  createReq.learnerReplicas[numOfLearnerReplicas].port = pDnode->port;
  memcpy(createReq.learnerReplicas[numOfLearnerReplicas].fqdn, pDnode->fqdn, TSDB_FQDN_LEN);

  createReq.lastIndex = pObj->lastIndex;

  createEpset.inUse = 0;
  createEpset.numOfEps = 1;
  createEpset.eps[0].port = pDnode->port;
  memcpy(createEpset.eps[0].fqdn, pDnode->fqdn, TSDB_FQDN_LEN);

  if (mndBuildCreateMnodeRedoAction(pTrans, &createReq, &createEpset) != 0) return -1;

  return 0;
}

int32_t mndSetRestoreCreateMnodeRedoActions(SMnode *pMnode, STrans *pTrans, SDnodeObj *pDnode, SMnodeObj *pObj) {
  SSdb            *pSdb = pMnode->pSdb;
  void            *pIter = NULL;
  SDCreateMnodeReq createReq = {0};
  SEpSet           createEpset = {0};

  while (1) {
    SMnodeObj *pMObj = NULL;
    pIter = sdbFetch(pSdb, SDB_MNODE, pIter, (void **)&pMObj);
    if (pIter == NULL) break;

    if (pMObj->id == pDnode->id) {
      sdbRelease(pSdb, pMObj);
      continue;
    }

    if (pMObj->role == TAOS_SYNC_ROLE_VOTER) {
      createReq.replicas[createReq.replica].id = pMObj->id;
      createReq.replicas[createReq.replica].port = pMObj->pDnode->port;
      memcpy(createReq.replicas[createReq.replica].fqdn, pMObj->pDnode->fqdn, TSDB_FQDN_LEN);
      createReq.replica++;
    } else {
      createReq.learnerReplicas[createReq.learnerReplica].id = pMObj->id;
      createReq.learnerReplicas[createReq.learnerReplica].port = pMObj->pDnode->port;
      memcpy(createReq.learnerReplicas[createReq.learnerReplica].fqdn, pMObj->pDnode->fqdn, TSDB_FQDN_LEN);
      createReq.learnerReplica++;
    }

    sdbRelease(pSdb, pMObj);
  }

  createReq.learnerReplicas[createReq.learnerReplica].id = pDnode->id;
  createReq.learnerReplicas[createReq.learnerReplica].port = pDnode->port;
  memcpy(createReq.learnerReplicas[createReq.learnerReplica].fqdn, pDnode->fqdn, TSDB_FQDN_LEN);
  createReq.learnerReplica++;

  createReq.lastIndex = pObj->lastIndex;

  createEpset.inUse = 0;
  createEpset.numOfEps = 1;
  createEpset.eps[0].port = pDnode->port;
  memcpy(createEpset.eps[0].fqdn, pDnode->fqdn, TSDB_FQDN_LEN);

  if (mndBuildCreateMnodeRedoAction(pTrans, &createReq, &createEpset) != 0) return -1;

  return 0;
}

static int32_t mndSetAlterMnodeTypeRedoActions(SMnode *pMnode, STrans *pTrans, SDnodeObj *pDnode, SMnodeObj *pObj) {
  SSdb               *pSdb = pMnode->pSdb;
  void               *pIter = NULL;
  SDAlterMnodeTypeReq alterReq = {0};
  SEpSet              createEpset = {0};

  while (1) {
    SMnodeObj *pMObj = NULL;
    pIter = sdbFetch(pSdb, SDB_MNODE, pIter, (void **)&pMObj);
    if (pIter == NULL) break;

    if (pMObj->role == TAOS_SYNC_ROLE_VOTER) {
      alterReq.replicas[alterReq.replica].id = pMObj->id;
      alterReq.replicas[alterReq.replica].port = pMObj->pDnode->port;
      memcpy(alterReq.replicas[alterReq.replica].fqdn, pMObj->pDnode->fqdn, TSDB_FQDN_LEN);
      alterReq.replica++;
    } else {
      alterReq.learnerReplicas[alterReq.learnerReplica].id = pMObj->id;
      alterReq.learnerReplicas[alterReq.learnerReplica].port = pMObj->pDnode->port;
      memcpy(alterReq.learnerReplicas[alterReq.learnerReplica].fqdn, pMObj->pDnode->fqdn, TSDB_FQDN_LEN);
      alterReq.learnerReplica++;
    }

    sdbRelease(pSdb, pMObj);
  }

  alterReq.replicas[alterReq.replica].id = pDnode->id;
  alterReq.replicas[alterReq.replica].port = pDnode->port;
  memcpy(alterReq.replicas[alterReq.replica].fqdn, pDnode->fqdn, TSDB_FQDN_LEN);
  alterReq.replica++;

  alterReq.lastIndex = pObj->lastIndex;

  createEpset.inUse = 0;
  createEpset.numOfEps = 1;
  createEpset.eps[0].port = pDnode->port;
  memcpy(createEpset.eps[0].fqdn, pDnode->fqdn, TSDB_FQDN_LEN);

  if (mndBuildAlterMnodeTypeRedoAction(pTrans, &alterReq, &createEpset) != 0) return -1;

  return 0;
}

int32_t mndSetRestoreAlterMnodeTypeRedoActions(SMnode *pMnode, STrans *pTrans, SDnodeObj *pDnode, SMnodeObj *pObj) {
  SSdb               *pSdb = pMnode->pSdb;
  void               *pIter = NULL;
  SDAlterMnodeTypeReq alterReq = {0};
  SEpSet              createEpset = {0};

  while (1) {
    SMnodeObj *pMObj = NULL;
    pIter = sdbFetch(pSdb, SDB_MNODE, pIter, (void **)&pMObj);
    if (pIter == NULL) break;

    if (pMObj->id == pDnode->id) {
      sdbRelease(pSdb, pMObj);
      continue;
    }

    if (pMObj->role == TAOS_SYNC_ROLE_VOTER) {
      alterReq.replicas[alterReq.replica].id = pMObj->id;
      alterReq.replicas[alterReq.replica].port = pMObj->pDnode->port;
      memcpy(alterReq.replicas[alterReq.replica].fqdn, pMObj->pDnode->fqdn, TSDB_FQDN_LEN);
      alterReq.replica++;
    } else {
      alterReq.learnerReplicas[alterReq.learnerReplica].id = pMObj->id;
      alterReq.learnerReplicas[alterReq.learnerReplica].port = pMObj->pDnode->port;
      memcpy(alterReq.learnerReplicas[alterReq.learnerReplica].fqdn, pMObj->pDnode->fqdn, TSDB_FQDN_LEN);
      alterReq.learnerReplica++;
    }

    sdbRelease(pSdb, pMObj);
  }

  alterReq.replicas[alterReq.replica].id = pDnode->id;
  alterReq.replicas[alterReq.replica].port = pDnode->port;
  memcpy(alterReq.replicas[alterReq.replica].fqdn, pDnode->fqdn, TSDB_FQDN_LEN);
  alterReq.replica++;

  alterReq.lastIndex = pObj->lastIndex;

  createEpset.inUse = 0;
  createEpset.numOfEps = 1;
  createEpset.eps[0].port = pDnode->port;
  memcpy(createEpset.eps[0].fqdn, pDnode->fqdn, TSDB_FQDN_LEN);

  if (mndBuildAlterMnodeTypeRedoAction(pTrans, &alterReq, &createEpset) != 0) return -1;

  return 0;
}

static int32_t mndCreateMnode(SMnode *pMnode, SRpcMsg *pReq, SDnodeObj *pDnode, SMCreateMnodeReq *pCreate) {
  int32_t code = -1;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_GLOBAL, pReq, "create-mnode");
  if (pTrans == NULL) goto _OVER;
  mndTransSetSerial(pTrans);
  mInfo("trans:%d, used to create mnode:%d", pTrans->id, pCreate->dnodeId);
  if (mndTransCheckConflict(pMnode, pTrans) != 0) goto _OVER;

  SMnodeObj mnodeObj = {0};
  mnodeObj.id = pDnode->id;
  mnodeObj.createdTime = taosGetTimestampMs();
  mnodeObj.updateTime = mnodeObj.createdTime;
  mnodeObj.role = TAOS_SYNC_ROLE_LEARNER;
  mnodeObj.lastIndex = pMnode->applied;

  if (mndSetCreateMnodeRedoActions(pMnode, pTrans, pDnode, &mnodeObj) != 0) goto _OVER;
  if (mndSetCreateMnodeRedoLogs(pMnode, pTrans, &mnodeObj) != 0) goto _OVER;

  SMnodeObj mnodeLeaderObj = {0};
  mnodeLeaderObj.id = pDnode->id;
  mnodeLeaderObj.createdTime = taosGetTimestampMs();
  mnodeLeaderObj.updateTime = mnodeLeaderObj.createdTime;
  mnodeLeaderObj.role = TAOS_SYNC_ROLE_VOTER;
  mnodeLeaderObj.lastIndex = pMnode->applied + 1;

  if (mndSetAlterMnodeTypeRedoActions(pMnode, pTrans, pDnode, &mnodeLeaderObj) != 0) goto _OVER;
  if (mndSetCreateMnodeCommitLogs(pMnode, pTrans, &mnodeLeaderObj) != 0) goto _OVER;
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
  SMCreateMnodeReq createReq = {0};

  if (tDeserializeSCreateDropMQSNodeReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("mnode:%d, start to create", createReq.dnodeId);
  if (mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CREATE_MNODE) != 0) {
    goto _OVER;
  }

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

  if (sdbGetSize(pMnode->pSdb, SDB_MNODE) >= 3) {
    terrno = TSDB_CODE_MND_TOO_MANY_MNODES;
    goto _OVER;
  }

  if (!mndIsDnodeOnline(pDnode, taosGetTimestampMs())) {
    terrno = TSDB_CODE_DNODE_OFFLINE;
    goto _OVER;
  }

  code = mndCreateMnode(pMnode, pReq, pDnode, &createReq);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  char obj[40] = {0};
  sprintf(obj, "%d", createReq.dnodeId);

  auditRecord(pReq, pMnode->clusterId, "createMnode", "", obj, createReq.sql, createReq.sqlLen);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("mnode:%d, failed to create since %s", createReq.dnodeId, terrstr());
  }

  mndReleaseMnode(pMnode, pObj);
  mndReleaseDnode(pMnode, pDnode);
  tFreeSMCreateQnodeReq(&createReq);

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

static int32_t mndSetDropMnodeRedoActions(SMnode *pMnode, STrans *pTrans, SDnodeObj *pDnode, SMnodeObj *pObj,
                                          bool force) {
  SSdb          *pSdb = pMnode->pSdb;
  void          *pIter = NULL;
  SDDropMnodeReq dropReq = {0};
  SEpSet         dropEpSet = {0};

  dropReq.dnodeId = pDnode->id;
  dropEpSet.numOfEps = 1;
  dropEpSet.eps[0].port = pDnode->port;
  memcpy(dropEpSet.eps[0].fqdn, pDnode->fqdn, TSDB_FQDN_LEN);

  int32_t totalMnodes = sdbGetSize(pSdb, SDB_MNODE);
  if (totalMnodes == 2) {
    if (force) {
      mError("cant't force drop dnode, since a mnode on it and replica is 2");
      terrno = TSDB_CODE_MNODE_ONLY_TWO_MNODE;
      return -1;
    }
    mInfo("vgId:1, has %d mnodes, exec redo log first", totalMnodes);
    if (mndSetDropMnodeRedoLogs(pMnode, pTrans, pObj) != 0) return -1;
    if (!force) {
      if (mndBuildDropMnodeRedoAction(pTrans, &dropReq, &dropEpSet) != 0) return -1;
    }
  } else if (totalMnodes == 3) {
    mInfo("vgId:1, has %d mnodes, exec redo action first", totalMnodes);
    if (!force) {
      if (mndBuildDropMnodeRedoAction(pTrans, &dropReq, &dropEpSet) != 0) return -1;
    }
    if (mndSetDropMnodeRedoLogs(pMnode, pTrans, pObj) != 0) return -1;
  } else {
    return -1;
  }

  return 0;
}

int32_t mndSetDropMnodeInfoToTrans(SMnode *pMnode, STrans *pTrans, SMnodeObj *pObj, bool force) {
  if (pObj == NULL) return 0;
  pObj->lastIndex = pMnode->applied;
  if (mndSetDropMnodeRedoActions(pMnode, pTrans, pObj->pDnode, pObj, force) != 0) return -1;
  if (mndSetDropMnodeCommitLogs(pMnode, pTrans, pObj) != 0) return -1;
  return 0;
}

static int32_t mndDropMnode(SMnode *pMnode, SRpcMsg *pReq, SMnodeObj *pObj) {
  int32_t code = -1;
  STrans *pTrans = NULL;

  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_GLOBAL, pReq, "drop-mnode");
  if (pTrans == NULL) goto _OVER;
  mndTransSetSerial(pTrans);
  mInfo("trans:%d, used to drop mnode:%d", pTrans->id, pObj->id);
  if (mndTransCheckConflict(pMnode, pTrans) != 0) goto _OVER;

  if (mndSetDropMnodeInfoToTrans(pMnode, pTrans, pObj, false) != 0) goto _OVER;
  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessDropMnodeReq(SRpcMsg *pReq) {
  SMnode        *pMnode = pReq->info.node;
  int32_t        code = -1;
  SMnodeObj     *pObj = NULL;
  SMDropMnodeReq dropReq = {0};

  if (tDeserializeSCreateDropMQSNodeReq(pReq->pCont, pReq->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("mnode:%d, start to drop", dropReq.dnodeId);
  if (mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_DROP_MNODE) != 0) {
    goto _OVER;
  }

  if (dropReq.dnodeId <= 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  pObj = mndAcquireMnode(pMnode, dropReq.dnodeId);
  if (pObj == NULL) {
    goto _OVER;
  }

  if (pMnode->selfDnodeId == dropReq.dnodeId) {
    terrno = TSDB_CODE_MND_CANT_DROP_LEADER;
    goto _OVER;
  }

  if (sdbGetSize(pMnode->pSdb, SDB_MNODE) <= 1) {
    terrno = TSDB_CODE_MND_TOO_FEW_MNODES;
    goto _OVER;
  }

  if (!mndIsDnodeOnline(pObj->pDnode, taosGetTimestampMs())) {
    terrno = TSDB_CODE_DNODE_OFFLINE;
    goto _OVER;
  }

  code = mndDropMnode(pMnode, pReq, pObj);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  char obj[40] = {0};
  sprintf(obj, "%d", dropReq.dnodeId);

  auditRecord(pReq, pMnode->clusterId, "dropMnode", "", obj, dropReq.sql, dropReq.sqlLen);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("mnode:%d, failed to drop since %s", dropReq.dnodeId, terrstr());
  }

  mndReleaseMnode(pMnode, pObj);
  tFreeSMCreateQnodeReq(&dropReq);
  return code;
}

static int32_t mndRetrieveMnodes(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode    *pMnode = pReq->info.node;
  SSdb      *pSdb = pMnode->pSdb;
  int32_t    numOfRows = 0;
  int32_t    cols = 0;
  SMnodeObj *pObj = NULL;
  SMnodeObj *pSelfObj = NULL;
  ESdbStatus objStatus = 0;
  char      *pWrite;
  int64_t    curMs = taosGetTimestampMs();

  pSelfObj = sdbAcquire(pSdb, SDB_MNODE, &pMnode->selfDnodeId);
  if (pSelfObj == NULL) {
    mError("mnode:%d, failed to acquire self %s", pMnode->selfDnodeId, terrstr());
    goto _out;
  }

  while (numOfRows < rows) {
    pShow->pIter = sdbFetchAll(pSdb, SDB_MNODE, pShow->pIter, (void **)&pObj, &objStatus, true);
    if (pShow->pIter == NULL) break;

    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->id, false);

    char b1[TSDB_EP_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(b1, pObj->pDnode->ep, TSDB_EP_LEN + VARSTR_HEADER_SIZE);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, b1, false);

    char role[20] = "offline";
    if (pObj->id == pMnode->selfDnodeId) {
      snprintf(role, sizeof(role), "%s%s", syncStr(TAOS_SYNC_STATE_LEADER), pMnode->restored ? "" : "*");
    }
    bool isDnodeOnline = mndIsDnodeOnline(pObj->pDnode, curMs);
    if (isDnodeOnline) {
      tstrncpy(role, syncStr(pObj->syncState), sizeof(role));
      if (pObj->syncState == TAOS_SYNC_STATE_LEADER && pObj->id != pMnode->selfDnodeId) {
        tstrncpy(role, syncStr(TAOS_SYNC_STATE_ERROR), sizeof(role));
        mError("mnode:%d, is leader too", pObj->id);
      }
    }
    char b2[12 + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(b2, role, pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)b2, false);

    const char *status = "ready";
    if (objStatus == SDB_STATUS_CREATING) status = "creating";
    if (objStatus == SDB_STATUS_DROPPING) status = "dropping";
    if (!isDnodeOnline) status = "offline";
    char b3[9 + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(b3, status, pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)b3, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->createdTime, false);

    int64_t roleTimeMs = (isDnodeOnline) ? pObj->roleTimeMs : 0;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&roleTimeMs, false);

    numOfRows++;
    sdbRelease(pSdb, pObj);
  }

  pShow->numOfRows += numOfRows;

_out:
  sdbRelease(pSdb, pSelfObj);
  return numOfRows;
}

static void mndCancelGetNextMnode(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}

static int32_t mndProcessAlterMnodeReq(SRpcMsg *pReq) {
#if 1
  return 0;
#else
  SMnode         *pMnode = pReq->info.node;
  SDAlterMnodeReq alterReq = {0};

  if (tDeserializeSDCreateMnodeReq(pReq->pCont, pReq->contLen, &alterReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  SMnodeOpt option = {.deploy = true, .numOfReplicas = alterReq.replica, .selfIndex = -1};
  memcpy(option.replicas, alterReq.replicas, sizeof(alterReq.replicas));
  for (int32_t i = 0; i < option.numOfReplicas; ++i) {
    if (alterReq.replicas[i].id == pMnode->selfDnodeId) {
      option.selfIndex = i;
    }
  }

  if (option.selfIndex == -1) {
    mInfo("alter mnode not processed since selfIndex is -1", terrstr());
    return 0;
  }

  if (mndWriteFile(pMnode->path, &option) != 0) {
    mError("failed to write mnode file since %s", terrstr());
    return -1;
  }

  SSyncCfg cfg = {.replicaNum = alterReq.replica, .myIndex = -1};
  for (int32_t i = 0; i < alterReq.replica; ++i) {
    SNodeInfo *pNode = &cfg.nodeInfo[i];
    tstrncpy(pNode->nodeFqdn, alterReq.replicas[i].fqdn, sizeof(pNode->nodeFqdn));
    pNode->nodePort = alterReq.replicas[i].port;
    if (alterReq.replicas[i].id == pMnode->selfDnodeId) {
      cfg.myIndex = i;
    }
  }

  if (cfg.myIndex == -1) {
    mError("failed to alter mnode since myindex is -1");
    return -1;
  } else {
    mInfo("start to alter mnode sync, replica:%d myIndex:%d", cfg.replicaNum, cfg.myIndex);
    for (int32_t i = 0; i < alterReq.replica; ++i) {
      SNodeInfo *pNode = &cfg.nodeInfo[i];
      mInfo("index:%d, fqdn:%s port:%d", i, pNode->nodeFqdn, pNode->nodePort);
    }
  }

  int32_t code = syncReconfig(pMnode->syncMgmt.sync, &cfg);
  if (code != 0) {
    mError("failed to sync reconfig since %s", terrstr());
  } else {
    mInfo("alter mnode sync success");
  }

  return code;
#endif
}

static void mndReloadSyncConfig(SMnode *pMnode) {
  SSdb      *pSdb = pMnode->pSdb;
  SMnodeObj *pObj = NULL;
  ESdbStatus objStatus = 0;
  void      *pIter = NULL;
  int32_t    updatingMnodes = 0;
  int32_t    readyMnodes = 0;
  SSyncCfg   cfg = {
        .myIndex = -1,
        .lastIndex = 0,
  };
  SyncIndex maxIndex = 0;

  while (1) {
    pIter = sdbFetchAll(pSdb, SDB_MNODE, pIter, (void **)&pObj, &objStatus, false);
    if (pIter == NULL) break;
    if (objStatus == SDB_STATUS_CREATING || objStatus == SDB_STATUS_DROPPING) {
      mInfo("vgId:1, has updating mnode:%d, status:%s", pObj->id, sdbStatusName(objStatus));
      updatingMnodes++;
    }
    if (objStatus == SDB_STATUS_READY) {
      mInfo("vgId:1, has ready mnode:%d, status:%s", pObj->id, sdbStatusName(objStatus));
      readyMnodes++;
    }

    if (objStatus == SDB_STATUS_READY || objStatus == SDB_STATUS_CREATING) {
      SNodeInfo *pNode = &cfg.nodeInfo[cfg.totalReplicaNum];
      pNode->nodeId = pObj->pDnode->id;
      pNode->clusterId = mndGetClusterId(pMnode);
      pNode->nodePort = pObj->pDnode->port;
      pNode->nodeRole = pObj->role;
      tstrncpy(pNode->nodeFqdn, pObj->pDnode->fqdn, TSDB_FQDN_LEN);
      (void)tmsgUpdateDnodeInfo(&pNode->nodeId, &pNode->clusterId, pNode->nodeFqdn, &pNode->nodePort);
      mInfo("vgId:1, ep:%s:%u dnode:%d", pNode->nodeFqdn, pNode->nodePort, pNode->nodeId);
      if (pObj->pDnode->id == pMnode->selfDnodeId) {
        cfg.myIndex = cfg.totalReplicaNum;
      }
      if (pNode->nodeRole == TAOS_SYNC_ROLE_VOTER) {
        cfg.replicaNum++;
      }
      cfg.totalReplicaNum++;
      if (pObj->lastIndex > cfg.lastIndex) {
        cfg.lastIndex = pObj->lastIndex;
      }
    }

    if (objStatus == SDB_STATUS_DROPPING) {
      if (pObj->lastIndex > cfg.lastIndex) {
        cfg.lastIndex = pObj->lastIndex;
      }
    }

    mInfo("vgId:1, mnode:%d, role:%d, lastIndex:%" PRId64, pObj->id, pObj->role, pObj->lastIndex);

    sdbReleaseLock(pSdb, pObj, false);
  }

  // if (readyMnodes <= 0 || updatingMnodes <= 0) {
  //   mInfo("vgId:1, mnode sync not reconfig since readyMnodes:%d updatingMnodes:%d", readyMnodes, updatingMnodes);
  //   return;
  // }

  if (cfg.myIndex == -1) {
#if 1
    mInfo("vgId:1, mnode sync not reconfig since selfIndex is -1");
#else
    // cannot reconfig because the leader may fail to elect after reboot
    mInfo("vgId:1, mnode sync not reconfig since selfIndex is -1, do sync stop oper");
    syncStop(pMnode->syncMgmt.sync);
#endif
    return;
  }

  if (pMnode->syncMgmt.sync > 0) {
    mInfo("vgId:1, mnode sync reconfig, totalReplica:%d replica:%d myIndex:%d", cfg.totalReplicaNum, cfg.replicaNum,
          cfg.myIndex);

    for (int32_t i = 0; i < cfg.totalReplicaNum; ++i) {
      SNodeInfo *pNode = &cfg.nodeInfo[i];
      mInfo("vgId:1, index:%d, ep:%s:%u dnode:%d cluster:%" PRId64 " role:%d", i, pNode->nodeFqdn, pNode->nodePort,
            pNode->nodeId, pNode->clusterId, pNode->nodeRole);
    }

    int32_t code = syncReconfig(pMnode->syncMgmt.sync, &cfg);
    if (code != 0) {
      mError("vgId:1, mnode sync reconfig failed since %s", terrstr());
    } else {
      mInfo("vgId:1, mnode sync reconfig success");
    }
  }
}
