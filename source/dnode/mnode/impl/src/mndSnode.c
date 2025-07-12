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
#include "mndDnode.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndSnode.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndStream.h"

#define SNODE_VER_NUMBER   1
#define SNODE_RESERVE_SIZE 64

static SSdbRaw *mndSnodeActionEncode(SSnodeObj *pObj);
static SSdbRow *mndSnodeActionDecode(SSdbRaw *pRaw);
static int32_t  mndSnodeActionInsert(SSdb *pSdb, SSnodeObj *pObj);
static int32_t  mndSnodeActionUpdate(SSdb *pSdb, SSnodeObj *pOld, SSnodeObj *pNew);
static int32_t  mndSnodeActionDelete(SSdb *pSdb, SSnodeObj *pObj);
static int32_t  mndProcessCreateSnodeReq(SRpcMsg *pReq);
static int32_t  mndProcessDropSnodeReq(SRpcMsg *pReq);
static int32_t  mndRetrieveSnodes(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextSnode(SMnode *pMnode, void *pIter);

int32_t mndInitSnode(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_SNODE,
      .keyType = SDB_KEY_INT32,
      .encodeFp = (SdbEncodeFp)mndSnodeActionEncode,
      .decodeFp = (SdbDecodeFp)mndSnodeActionDecode,
      .insertFp = (SdbInsertFp)mndSnodeActionInsert,
      .updateFp = (SdbUpdateFp)mndSnodeActionUpdate,
      .deleteFp = (SdbDeleteFp)mndSnodeActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_SNODE, mndProcessCreateSnodeReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_SNODE, mndProcessDropSnodeReq);
  mndSetMsgHandle(pMnode, TDMT_DND_CREATE_SNODE_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_DND_ALTER_SNODE_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_DND_DROP_SNODE_RSP, mndTransProcessRsp);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_SNODE, mndRetrieveSnodes);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_SNODE, mndCancelGetNextSnode);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupSnode(SMnode *pMnode) {}

SEpSet mndAcquireEpFromSnode(SMnode *pMnode, const SSnodeObj *pSnode) {
  SEpSet epSet = {.numOfEps = 1, .inUse = 0};
  memcpy(epSet.eps[0].fqdn, pSnode->pDnode->fqdn, TSDB_FQDN_LEN);
  epSet.eps[0].port = pSnode->pDnode->port;
  return epSet;
}

SSnodeObj *mndAcquireSnode(SMnode *pMnode, int32_t snodeId) {
  SSnodeObj *pObj = sdbAcquire(pMnode->pSdb, SDB_SNODE, &snodeId);
  if (pObj == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_MND_SNODE_NOT_EXIST;
  }
  return pObj;
}

void mndReleaseSnode(SMnode *pMnode, SSnodeObj *pObj) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pObj);
}

static SSdbRaw *mndSnodeActionEncode(SSnodeObj *pObj) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_SNODE, SNODE_VER_NUMBER, sizeof(SSnodeObj) + SNODE_RESERVE_SIZE);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pObj->id, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->leadersId[0], _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->leadersId[1], _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->replicaId, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->createdTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->updateTime, _OVER)
  SDB_SET_RESERVE(pRaw, dataPos, SNODE_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("snode:%d, failed to encode to raw:%p since %s", pObj->id, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("snode:%d, encode to raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRaw;
}

static SSdbRow *mndSnodeActionDecode(SSdbRaw *pRaw) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  SSdbRow   *pRow = NULL;
  SSnodeObj *pObj = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver != SNODE_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  pRow = sdbAllocRow(sizeof(SSnodeObj));
  if (pRow == NULL) goto _OVER;

  pObj = sdbGetRowObj(pRow);
  if (pObj == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &pObj->id, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pObj->leadersId[0], _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pObj->leadersId[1], _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pObj->replicaId, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->createdTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->updateTime, _OVER)
  SDB_GET_RESERVE(pRaw, dataPos, SNODE_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("snode:%d, failed to decode from raw:%p since %s", pObj == NULL ? 0 : pObj->id, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mDebug("snode:%d, decode from raw:%p, row:%p, leaderId[0]:%d, leaderId[1]:%d, replicaId:%d", pObj->id, pRaw, pObj, pObj->leadersId[0], pObj->leadersId[1], pObj->replicaId);
  return pRow;
}

static int32_t mndSnodeActionInsert(SSdb *pSdb, SSnodeObj *pObj) {
  mTrace("snode:%d, perform insert action, row:%p", pObj->id, pObj);
  pObj->pDnode = sdbAcquire(pSdb, SDB_DNODE, &pObj->id);
  if (pObj->pDnode == NULL) {
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    mError("snode:%d, failed to perform insert action since %s", pObj->id, terrstr());
    return -1;
  }

  return 0;
}

static int32_t mndSnodeActionDelete(SSdb *pSdb, SSnodeObj *pObj) {
  mTrace("snode:%d, perform delete action, row:%p", pObj->id, pObj);
  if (pObj->pDnode != NULL) {
    sdbRelease(pSdb, pObj->pDnode);
    pObj->pDnode = NULL;
  }

  return 0;
}

static int32_t mndSnodeActionUpdate(SSdb *pSdb, SSnodeObj *pOld, SSnodeObj *pNew) {
  mDebug("snode:%d, perform update action, old row:%p new row:%p", pOld->id, pOld, pNew);
  pOld->leadersId[0] = pNew->leadersId[0];
  pOld->leadersId[1] = pNew->leadersId[1];
  pOld->replicaId = pNew->replicaId;
  pOld->updateTime = pNew->updateTime;
  return 0;
}

static int32_t mndSetCreateSnodeRedoLogs(STrans *pTrans, SSnodeObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndSnodeActionEncode(pObj);
  if (pRedoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendRedolog(pTrans, pRedoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING));
  TAOS_RETURN(code);
}

static int32_t mndSetUpdateSnodeRedoLogs(STrans *pTrans, SSnodeObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndSnodeActionEncode(pObj);
  if (pRedoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendGroupRedolog(pTrans, pRedoRaw, -1));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_UPDATE));
  TAOS_RETURN(code);
}


static int32_t mndSetCreateSnodeUndoLogs(STrans *pTrans, SSnodeObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pUndoRaw = mndSnodeActionEncode(pObj);
  if (pUndoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendUndolog(pTrans, pUndoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED));
  TAOS_RETURN(code);
}

static int32_t mndSetCreateSnodeCommitLogs(STrans *pTrans, SSnodeObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndSnodeActionEncode(pObj);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pCommitRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));
  TAOS_RETURN(code);
}

static int32_t mndSetCreateSnodeRedoActions(STrans *pTrans, SDnodeObj *pDnode, SSnodeObj *pObj, SMnode *pMnode) {
  int32_t          code = 0;
  SDCreateSnodeReq createReq = {0};
  createReq.snodeId = pDnode->id;
  createReq.leaders[0].nodeId = pObj->leadersId[0];
  if (createReq.leaders[0].nodeId > 0) {
    createReq.leaders[0].epSet = mndGetDnodeEpsetById(pMnode, createReq.leaders[0].nodeId);
  }
  createReq.leaders[1].nodeId = pObj->leadersId[1];
  if (createReq.leaders[1].nodeId > 0) {
    createReq.leaders[1].epSet = mndGetDnodeEpsetById(pMnode, createReq.leaders[1].nodeId);
  }
  createReq.replica.nodeId = pObj->replicaId;
  if (createReq.replica.nodeId > 0) {
    createReq.replica.epSet = mndGetDnodeEpsetById(pMnode, createReq.replica.nodeId);
  }

  int32_t contLen = tSerializeSDCreateSNodeReq(NULL, 0, &createReq);
  void   *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    code = terrno;
    TAOS_RETURN(code);
  }
  code = tSerializeSDCreateSNodeReq(pReq, contLen, &createReq);
  if (code < 0) {
    mError("snode:%d, failed to serialize create drop snode request since %s", createReq.snodeId, terrstr());
  }

  STransAction action = {0};
  action.epSet = mndGetDnodeEpset(pDnode);
  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_CREATE_SNODE;
  action.acceptableCode = TSDB_CODE_SNODE_ALREADY_DEPLOYED;

  if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pReq);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

static int32_t mndSetUpdateSnodeRedoActions(STrans *pTrans, SDnodeObj *pDnode, SSnodeObj *pObj, SMnode *pMnode) {
  int32_t          code = 0;
  SDCreateSnodeReq createReq = {0};
  createReq.snodeId = pDnode->id;
  createReq.leaders[0].nodeId = pObj->leadersId[0];
  if (createReq.leaders[0].nodeId > 0) {
    createReq.leaders[0].epSet = mndGetDnodeEpsetById(pMnode, createReq.leaders[0].nodeId);
  }
  createReq.leaders[1].nodeId = pObj->leadersId[1];
  if (createReq.leaders[1].nodeId > 0) {
    createReq.leaders[1].epSet = mndGetDnodeEpsetById(pMnode, createReq.leaders[1].nodeId);
  }
  createReq.replica.nodeId = pObj->replicaId;
  if (createReq.replica.nodeId > 0) {
    createReq.replica.epSet = mndGetDnodeEpsetById(pMnode, createReq.replica.nodeId);
  }

  int32_t contLen = tSerializeSDCreateSNodeReq(NULL, 0, &createReq);
  void   *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    code = terrno;
    TAOS_RETURN(code);
  }
  code = tSerializeSDCreateSNodeReq(pReq, contLen, &createReq);
  if (code < 0) {
    mError("snode:%d, failed to serialize create drop snode request since %s", createReq.snodeId, terrstr());
  }

  STransAction action = {0};
  action.epSet = mndGetDnodeEpset(pDnode);
  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_ALTER_SNODE;
  action.acceptableCode = 0;
  action.groupId = -1;

  if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pReq);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}


static int32_t mndSetCreateSnodeUndoActions(STrans *pTrans, SDnodeObj *pDnode, SSnodeObj *pObj) {
  int32_t        code = 0;
  SDDropSnodeReq dropReq = {0};
  dropReq.dnodeId = pDnode->id;

  int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &dropReq);
  void   *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    code = terrno;
    TAOS_RETURN(code);
  }
  code = tSerializeSCreateDropMQSNodeReq(pReq, contLen, &dropReq);
  if (code < 0) {
    mError("snode:%d, failed to serialize create drop snode request since %s", dropReq.dnodeId, terrstr());
  }

  STransAction action = {0};
  action.epSet = mndGetDnodeEpset(pDnode);
  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_DROP_SNODE;
  action.acceptableCode = TSDB_CODE_SNODE_NOT_DEPLOYED;

  if ((code = mndTransAppendUndoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pReq);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

static int32_t mndCreateSnode(SMnode *pMnode, SRpcMsg *pReq, SDnodeObj *pDnode, SMCreateSnodeReq *pCreate, int64_t currTs) {
  int32_t code = -1;

  SSnodeObj snodeObj = {0};
  snodeObj.id = pDnode->id;
  snodeObj.createdTime = currTs;
  snodeObj.updateTime = snodeObj.createdTime;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "create-snode");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }
  mndTransSetSerial(pTrans);

  mInfo("trans:%d, used to create snode:%d", pTrans->id, pCreate->dnodeId);

  TAOS_CHECK_GOTO(mndSetCreateSnodeRedoLogs(pTrans, &snodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateSnodeUndoLogs(pTrans, &snodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateSnodeCommitLogs(pTrans, &snodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateSnodeRedoActions(pTrans, pDnode, &snodeObj, pMnode), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateSnodeUndoActions(pTrans, pDnode, &snodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static bool mndSnodeTraverseForCreate(SMnode *pMnode, void *pObj, void *p1, void *p2, void *p3) {
  SSnodeObj* pSnode = pObj;
  SSnodeObj* pReplica = (SSnodeObj*)p1;
  SSnodeObj* pLeader = (SSnodeObj*)p2;
  bool* noLeaderGot = (bool*)p3;

  if (0 == pSnode->leadersId[0]) {
    *pReplica = *pSnode;
    if (pLeader->id > 0) {
      return false;
    }

    *noLeaderGot = true;
  } else if (0 == pReplica->id && 0 == pSnode->leadersId[1]) {
    *pReplica = *pSnode;
  }

  if (0 == pSnode->replicaId && 0 == pLeader->id) {
    *pLeader = *pSnode;
    if (*noLeaderGot) {
      return false;
    }
  }

  return true;
}


void mndSnodeGetReplicaSnode(SMnode *pMnode, SMCreateSnodeReq *pCreate, SSnodeObj *pReplica, SSnodeObj* pLeader) {
  bool noLeaderGot = false;
  sdbTraverse(pMnode->pSdb, SDB_SNODE, mndSnodeTraverseForCreate, pReplica, pLeader, &noLeaderGot);
}

static int32_t mndSnodeAppendUpdateToTrans(SMnode *pMnode, SSnodeObj* pObj, STrans *pTrans) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SDnodeObj *pDnode2 = mndAcquireDnode(pMnode, pObj->id);
  if (pDnode2 == NULL) {
    code = TSDB_CODE_MND_DNODE_NOT_EXIST;
    goto _exit;
  }
  
  SSnodeObj snodeObj2 = {0};
  snodeObj2.id = pObj->id;
  snodeObj2.leadersId[0] = pObj->leadersId[0];
  snodeObj2.leadersId[1] = pObj->leadersId[1];
  snodeObj2.replicaId = pObj->replicaId;
  snodeObj2.createdTime = pObj->createdTime;
  snodeObj2.updateTime = taosGetTimestampMs();
  
  mInfo("trans:%d, used to update snode:%d", pTrans->id, snodeObj2.id);
  
  TAOS_CHECK_EXIT(mndSetUpdateSnodeRedoLogs(pTrans, &snodeObj2));
  //TAOS_CHECK_EXIT(mndSetCreateSnodeUndoLogs(pTrans, &snodeObj2));
  TAOS_CHECK_EXIT(mndSetCreateSnodeCommitLogs(pTrans, &snodeObj2));
  TAOS_CHECK_EXIT(mndSetUpdateSnodeRedoActions(pTrans, pDnode2, &snodeObj2, pMnode));
  //TAOS_CHECK_EXIT(mndSetCreateSnodeUndoActions(pTrans, pDnode2, &snodeObj2));

_exit:

  mndReleaseDnode(pMnode, pDnode2);

  if (code) {
    mError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;  
}

static int32_t mndCreateSnodeWithReplicaId(SMnode *pMnode, SRpcMsg *pReq, SDnodeObj *pDnode, SMCreateSnodeReq *pCreate, int64_t currTs) {
  SSnodeObj replicaSnode = {0};
  SSnodeObj leaderSnode = {0};
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  mndSnodeGetReplicaSnode(pMnode, pCreate, &replicaSnode, &leaderSnode);

  SSnodeObj snodeObj = {0};
  snodeObj.id = pDnode->id;
  if (leaderSnode.id > 0) {
    snodeObj.leadersId[0] = leaderSnode.id;
  }
  snodeObj.replicaId = replicaSnode.id;
  snodeObj.createdTime = currTs;
  snodeObj.updateTime = snodeObj.createdTime;

  if (leaderSnode.id > 0) {
    leaderSnode.replicaId = snodeObj.id;
  }

  if (replicaSnode.id > 0) {
    SSnodeObj* pTarget = &replicaSnode;
    if (replicaSnode.id == leaderSnode.id) {
      pTarget = &leaderSnode;
    }
    if (pTarget->leadersId[0] > 0) {
      pTarget->leadersId[1] = snodeObj.id;
    } else {
      pTarget->leadersId[0] = snodeObj.id;
    }
  }

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "create-snode");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _exit;
  }
  
  mndTransSetSerial(pTrans);

  mInfo("trans:%d, used to create snode:%d", pTrans->id, pCreate->dnodeId);

  TAOS_CHECK_EXIT(mndSetCreateSnodeRedoLogs(pTrans, &snodeObj));
  TAOS_CHECK_EXIT(mndSetCreateSnodeUndoLogs(pTrans, &snodeObj));
  TAOS_CHECK_EXIT(mndSetCreateSnodeCommitLogs(pTrans, &snodeObj));
  TAOS_CHECK_EXIT(mndSetCreateSnodeRedoActions(pTrans, pDnode, &snodeObj, pMnode));
  TAOS_CHECK_EXIT(mndSetCreateSnodeUndoActions(pTrans, pDnode, &snodeObj));

  if (leaderSnode.id > 0) {
    TAOS_CHECK_EXIT(mndSnodeAppendUpdateToTrans(pMnode, &leaderSnode, pTrans));
  }

  if (replicaSnode.id > 0 && replicaSnode.id != leaderSnode.id) {
    TAOS_CHECK_EXIT(mndSnodeAppendUpdateToTrans(pMnode, &replicaSnode, pTrans));
  }

  TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));

_exit:

  mndTransDrop(pTrans);

  if (code) {
    mError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
  TAOS_RETURN(code);
}


static int32_t mndProcessCreateSnodeReq(SRpcMsg *pReq) {
  SMnode          *pMnode = pReq->info.node;
  int32_t          code = -1;
  SSnodeObj       *pObj = NULL;
  SDnodeObj       *pDnode = NULL;
  SMCreateSnodeReq createReq = {0};

  TAOS_CHECK_GOTO(tDeserializeSCreateDropMQSNodeReq(pReq->pCont, pReq->contLen, &createReq), NULL, _OVER);

  mInfo("snode:%d, start to create", createReq.dnodeId);
  
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CREATE_SNODE), NULL, _OVER);

  pObj = mndAcquireSnode(pMnode, createReq.dnodeId);
  if (pObj != NULL) {
    code = TSDB_CODE_MND_SNODE_ALREADY_EXIST;
    mndReleaseSnode(pMnode, pObj);
    goto _OVER;
  } else if (terrno != TSDB_CODE_MND_SNODE_NOT_EXIST) {
    goto _OVER;
  }

  pDnode = mndAcquireDnode(pMnode, createReq.dnodeId);
  if (pDnode == NULL) {
    code = TSDB_CODE_MND_DNODE_NOT_EXIST;
    goto _OVER;
  }

  int64_t currTs = taosGetTimestampMs();
  int32_t snodeNum = sdbGetSize(pMnode->pSdb, SDB_SNODE);
  if (snodeNum > 0) {
    code = mndCreateSnodeWithReplicaId(pMnode, pReq, pDnode, &createReq, currTs);
  } else {
    code = mndCreateSnode(pMnode, pReq, pDnode, &createReq, currTs);
  }
  
  if (code == 0) {
    code = TSDB_CODE_ACTION_IN_PROGRESS;
    
    MND_STREAM_SET_LAST_TS(STM_EVENT_CREATE_SNODE, currTs);
  }

_OVER:

  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("snode:%d, failed to create since %s", createReq.dnodeId, tstrerror(code));
  }

  mndReleaseDnode(pMnode, pDnode);
  tFreeSMCreateQnodeReq(&createReq);
  TAOS_RETURN(code);
}

static int32_t mndSetDropSnodeRedoLogs(STrans *pTrans, SSnodeObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndSnodeActionEncode(pObj);
  if (pRedoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendGroupRedolog(pTrans, pRedoRaw, -1));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING));
  TAOS_RETURN(code);
}

static int32_t mndSetDropSnodeCommitLogs(STrans *pTrans, SSnodeObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndSnodeActionEncode(pObj);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pCommitRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED));
  TAOS_RETURN(code);
}

static int32_t mndSetDropSnodeRedoActions(STrans *pTrans, SDnodeObj *pDnode, SSnodeObj *pObj) {
  int32_t        code = 0;
  SDDropSnodeReq dropReq = {0};
  dropReq.dnodeId = pDnode->id;

  int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &dropReq);
  void   *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    code = terrno;
    TAOS_RETURN(code);
  }
  code = tSerializeSCreateDropMQSNodeReq(pReq, contLen, &dropReq);
  if (code < 0) {
    mError("snode:%d, failed to serialize create drop snode request since %s", dropReq.dnodeId, terrstr());
  }

  STransAction action = {0};
  action.epSet = mndGetDnodeEpset(pDnode);
  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_DROP_SNODE;
  action.acceptableCode = TSDB_CODE_SNODE_NOT_DEPLOYED;
  action.groupId = -1;

  if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pReq);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

int32_t mndSetDropSnodeInfoToTrans(SMnode *pMnode, STrans *pTrans, SSnodeObj *pObj, bool force) {
  if (pObj == NULL) return 0;
  TAOS_CHECK_RETURN(mndSetDropSnodeRedoLogs(pTrans, pObj));
  TAOS_CHECK_RETURN(mndSetDropSnodeCommitLogs(pTrans, pObj));
  if (!force) {
    TAOS_CHECK_RETURN(mndSetDropSnodeRedoActions(pTrans, pObj->pDnode, pObj));
  }
  return 0;
}

static bool mndSnodeTraverseForDropAff(SMnode *pMnode, void *pObj, void *p1, void *p2, void *p3) {
  SSnodeObj* pSnode = pObj;
  SSnodeDropTraversaCtx* ctx = (SSnodeDropTraversaCtx*)p1;

  if (pSnode->replicaId == ctx->target->id) {
    if (ctx->affNum <= 1) {
      ctx->affSnode[ctx->affNum++] = *pSnode;
    } else {
      mError("%d snodes with same replicaId:%d, %d, %d, %d", ctx->affNum + 1, ctx->target->id, 
          ctx->affSnode[0].id, ctx->affSnode[1].id, pSnode->id);
    }
  }

  return true;
}

static bool mndSnodeTraverseForNewReplica(SMnode *pMnode, void *pObj, void *p1, void *p2, void *p3) {
  SSnodeObj* pSnode = pObj;
  SSnodeObj* pSnode1 = (SSnodeObj*)p1;
  SSnodeObj* pSnode2 = (SSnodeObj*)p2;

  if (pSnode->id == pSnode1->id || pSnode->id == pSnode2->id) {
    return true;
  }

  int32_t n = pSnode->leadersId[0] ? (pSnode->leadersId[1] ? 2 : 1) : 0;
  if (0 == n) {
    *(SSnodeObj*)p3 = *pSnode;
    return false;
  } else if (1 == n && 0 == ((SSnodeObj*)p3)->id) {
    *(SSnodeObj*)p3 = *pSnode;
  }

  return true;
}


static int32_t mndSnodeCheckDropReplica(SMnode *pMnode, SSnodeObj* pObj, SSnodeDropTraversaCtx* pCtx) {
  int32_t snodeNum = sdbGetSize(pMnode->pSdb, SDB_SNODE);
  if (snodeNum <= 1) {
    mWarn("no need to get drop snode replica, replicaId:%d, remainSnode:%d", pObj->replicaId, snodeNum);
    return TSDB_CODE_SUCCESS;
  }

  sdbTraverse(pMnode->pSdb, SDB_SNODE, mndSnodeTraverseForDropAff, pCtx, NULL, NULL);

  if (0 == pCtx->affNum) {
    mDebug("no affected snode for dropping snode %d", pObj->id);
    return TSDB_CODE_SUCCESS;
  }

  if (2 == pCtx->affNum) {
    sdbTraverse(pMnode->pSdb, SDB_SNODE, mndSnodeTraverseForNewReplica, pCtx->target, &pCtx->affSnode[0], &pCtx->affNewReplica[0]);
    sdbTraverse(pMnode->pSdb, SDB_SNODE, mndSnodeTraverseForNewReplica, pCtx->target, &pCtx->affSnode[1], &pCtx->affNewReplica[1]);

    mDebug("two affected snodes for dropping snode %d, first:%d newRId:%d, sencod:%d newRId:%d", 
        pObj->id, pCtx->affSnode[0].id, pCtx->affNewReplica[0].id, pCtx->affSnode[1].id, pCtx->affNewReplica[1].id);

    return TSDB_CODE_SUCCESS;
  }

  SSnodeObj* replicaSnode = mndAcquireSnode(pMnode, pObj->replicaId);
  pCtx->affNewReplica[0] = *replicaSnode;
  mndReleaseSnode(pMnode, replicaSnode);
  
  mDebug("one affected snodes for dropping snode %d, first:%d newRId:%d", pObj->id, pCtx->affSnode[0].id, pCtx->affNewReplica[0].id);

  return TSDB_CODE_SUCCESS;
}

static void mndSnodeRemoveLeaderId(SSnodeObj *pObj, int32_t leaderId) {
  if (NULL == pObj || 0 == pObj->id) {
    return;
  }
  
  if (pObj->leadersId[1] == leaderId) {
    pObj->leadersId[1] = 0;
    return;
  }

  if (pObj->leadersId[0] == leaderId) {
    if (pObj->leadersId[1] > 0) {
      pObj->leadersId[0] = pObj->leadersId[1];
      pObj->leadersId[1] = 0;
      return;
    }

    pObj->leadersId[0] = 0;
  }
}

static void mndSnodeAddLeaderId(SSnodeObj *pObj, int32_t leaderId) {
  if (NULL == pObj || 0 == pObj->id || 0 == leaderId) {
    return;
  }
  
  if (0 == pObj->leadersId[0]) {
    pObj->leadersId[0] = leaderId;
    return;
  }

  if (0 == pObj->leadersId[1]) {
    pObj->leadersId[1] = leaderId;
    return;
  }

  mError("snode %d can't add new leaderId %d since already has two leaders:%d, %d", pObj->id, leaderId, pObj->leadersId[0], pObj->leadersId[1]);
}

int32_t mndDropSnodeImpl(SMnode *pMnode, SRpcMsg *pReq, SSnodeObj *pObj, STrans *pTrans) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SArray* streamList = NULL;
  SSnodeObj asReplicaOf[2] = {0};
  SSnodeObj replicaSnode = {0};
  SSnodeObj* pTmp = NULL;

  if (pObj->leadersId[0] > 0) {
    pTmp = mndAcquireSnode(pMnode, pObj->leadersId[0]);
    TSDB_CHECK_NULL(pTmp, code, lino, _exit, terrno);
    asReplicaOf[0] = *pTmp;
    mndReleaseSnode(pMnode, pTmp);
  }  

  if (pObj->leadersId[1] > 0) {
    pTmp = mndAcquireSnode(pMnode, pObj->leadersId[1]);
    TSDB_CHECK_NULL(pTmp, code, lino, _exit, terrno);
    asReplicaOf[1] = *pTmp;
    mndReleaseSnode(pMnode, pTmp);
  }

  if (pObj->replicaId > 0 && pObj->replicaId != pObj->leadersId[0] && pObj->replicaId != pObj->leadersId[1]) {
    pTmp = mndAcquireSnode(pMnode, pObj->replicaId);
    TSDB_CHECK_NULL(pTmp, code, lino, _exit, terrno);
    replicaSnode = *pTmp;
    mndReleaseSnode(pMnode, pTmp);
  }
  
  TAOS_CHECK_EXIT(msmCheckSnodeReassign(pMnode, pObj, &streamList));

  int32_t streamNum = taosArrayGetSize(streamList);
  if (streamNum > 0) {
    for (int32_t i = 0; i < streamNum; ++i) {
      SStreamObj* pStream = taosArrayGetP(streamList, i);
      atomic_store_32(&pStream->mainSnodeId, pObj->replicaId);
      TAOS_CHECK_EXIT(mndStreamTransAppend(pStream, pTrans, SDB_STATUS_READY));
    }
  }

  mndSnodeRemoveLeaderId(&asReplicaOf[0], pObj->id);
  mndSnodeRemoveLeaderId(&asReplicaOf[1], pObj->id);
  mndSnodeRemoveLeaderId(&replicaSnode, pObj->id);

  if (asReplicaOf[0].id > 0) {
    if (asReplicaOf[1].id > 0) {
      asReplicaOf[0].replicaId = asReplicaOf[1].id;
      asReplicaOf[1].replicaId = asReplicaOf[0].id;
      mndSnodeAddLeaderId(&asReplicaOf[0], asReplicaOf[1].id);
      mndSnodeAddLeaderId(&asReplicaOf[1], asReplicaOf[0].id);
    } else {
      if (0 == replicaSnode.id) {
        sdbTraverse(pMnode->pSdb, SDB_SNODE, mndSnodeTraverseForNewReplica, pObj, &asReplicaOf[0], &replicaSnode);
      }
      
      asReplicaOf[0].replicaId = replicaSnode.id;
      mndSnodeAddLeaderId(&replicaSnode, asReplicaOf[0].id);
    }
  }

  if (asReplicaOf[0].id > 0) {
    mInfo("trans:%d, used to update snode:%d", pTrans->id, asReplicaOf[0].id);
    TAOS_CHECK_EXIT(mndSnodeAppendUpdateToTrans(pMnode, &asReplicaOf[0], pTrans));
  }
  if (asReplicaOf[1].id > 0) {
    mInfo("trans:%d, used to update snode:%d", pTrans->id, asReplicaOf[1].id);
    TAOS_CHECK_EXIT(mndSnodeAppendUpdateToTrans(pMnode, &asReplicaOf[1], pTrans));
  }
  if (replicaSnode.id > 0) {
    mInfo("trans:%d, used to update snode:%d", pTrans->id, replicaSnode.id);
    TAOS_CHECK_EXIT(mndSnodeAppendUpdateToTrans(pMnode, &replicaSnode, pTrans));
  }

  mInfo("trans:%d, used to drop snode:%d", pTrans->id, pObj->id);
  TAOS_CHECK_GOTO(mndSetDropSnodeInfoToTrans(pMnode, pTrans, pObj, false), NULL, _exit);
  
_exit:

  taosArrayDestroy(streamList);

  if (code) {
    mError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
  TAOS_RETURN(code);
}


static int32_t mndDropSnode(SMnode *pMnode, SRpcMsg *pReq, SSnodeObj *pObj) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq, "drop-snode");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _exit;
  }
  mndTransSetSerial(pTrans);

  TAOS_CHECK_GOTO(mndDropSnodeImpl(pMnode, pReq, pObj, pTrans), &lino, _exit);
  
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), &lino, _exit);

_exit:

  mndTransDrop(pTrans);

  if (code) {
    mError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  
  TAOS_RETURN(code);
}

static int32_t mndProcessDropSnodeReq(SRpcMsg *pReq) {
  SMnode        *pMnode = pReq->info.node;
  int32_t        code = -1;
  SSnodeObj     *pObj = NULL;
  SMDropSnodeReq dropReq = {0};

  TAOS_CHECK_GOTO(tDeserializeSCreateDropMQSNodeReq(pReq->pCont, pReq->contLen, &dropReq), NULL, _OVER);

  mInfo("snode:%d, start to drop", dropReq.dnodeId);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_DROP_SNODE), NULL, _OVER);

  if (dropReq.dnodeId <= 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  pObj = mndAcquireSnode(pMnode, dropReq.dnodeId);
  if (pObj == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }

  // check deletable
  code = mndDropSnode(pMnode, pReq, pObj);
  if (code == 0) {
    code = TSDB_CODE_ACTION_IN_PROGRESS;

    MND_STREAM_SET_LAST_TS(STM_EVENT_DROP_SNODE, taosGetTimestampMs());
  }

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("snode:%d, failed to drop since %s", dropReq.dnodeId, tstrerror(code));
  }

  mndReleaseSnode(pMnode, pObj);
  tFreeSMCreateQnodeReq(&dropReq);
  TAOS_RETURN(code);
}

static int32_t mndRetrieveSnodes(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode    *pMnode = pReq->info.node;
  SSdb      *pSdb = pMnode->pSdb;
  int32_t    numOfRows = 0;
  int32_t    cols = 0;
  SSnodeObj *pObj = NULL;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_SNODE, pShow->pIter, (void **)&pObj);
    if (pShow->pIter == NULL) break;

    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_RETURN_WITH_RELEASE(colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->id, false), pSdb, pObj);

    char ep[TSDB_EP_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(ep, pObj->pDnode->ep, pShow->pMeta->pSchemas[cols].bytes);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_RETURN_WITH_RELEASE(colDataSetVal(pColInfo, numOfRows, (const char *)ep, false), pSdb, pObj);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_RETURN_WITH_RELEASE(colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->createdTime, false), pSdb,
                                   pObj);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_RETURN_WITH_RELEASE(colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->replicaId, false), pSdb,
                                   pObj);

    char asReplicaOf[64 + VARSTR_HEADER_SIZE] = {0};
    if (pObj->leadersId[0] && pObj->leadersId[1]) {
      snprintf(varDataVal(asReplicaOf), sizeof(asReplicaOf) - VARSTR_HEADER_SIZE, "%d, %d", pObj->leadersId[0], pObj->leadersId[1]);
    } else if (pObj->leadersId[0] || pObj->leadersId[1]) {
      snprintf(varDataVal(asReplicaOf), sizeof(asReplicaOf) - VARSTR_HEADER_SIZE, "%d", pObj->leadersId[0] ? pObj->leadersId[0] : pObj->leadersId[1]);
    } else {
      strcpy(varDataVal(asReplicaOf), "None");
    }
    varDataSetLen(asReplicaOf, strlen(varDataVal(asReplicaOf)));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_RETURN_WITH_RELEASE(colDataSetVal(pColInfo, numOfRows, (const char *)asReplicaOf, false), pSdb,
                                  pObj);

    numOfRows++;
    sdbRelease(pSdb, pObj);
  }

  pShow->numOfRows += numOfRows;

  return numOfRows;
}

static void mndCancelGetNextSnode(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_SNODE);
}
