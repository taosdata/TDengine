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

  mTrace("snode:%d, decode from raw:%p, row:%p", pObj->id, pRaw, pObj);
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
  mTrace("snode:%d, perform update action, old row:%p new row:%p", pOld->id, pOld, pNew);
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
  TAOS_CHECK_RETURN(mndTransAppendRedolog(pTrans, pRedoRaw));
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

static int32_t mndSetCreateSnodeRedoActions(STrans *pTrans, SDnodeObj *pDnode, SSnodeObj *pObj, int32_t replicaId) {
  int32_t          code = 0;
  SDCreateSnodeReq createReq = {0};
  createReq.snodeId = pDnode->id;
  createReq.replicaId = replicaId;

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

static int32_t mndSetUpdateSnodeRedoActions(STrans *pTrans, SDnodeObj *pDnode, SSnodeObj *pObj, int32_t replicaId) {
  int32_t          code = 0;
  SDCreateSnodeReq createReq = {0};
  createReq.snodeId = pDnode->id;
  createReq.replicaId = replicaId;

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

static int32_t mndCreateSnode(SMnode *pMnode, SRpcMsg *pReq, SDnodeObj *pDnode, SMCreateSnodeReq *pCreate, int32_t replicaId, int64_t currTs) {
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
  TAOS_CHECK_GOTO(mndSetCreateSnodeRedoActions(pTrans, pDnode, &snodeObj, replicaId), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateSnodeUndoActions(pTrans, pDnode, &snodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static bool mndSnodeTraverseForCreate(SMnode *pMnode, void *pObj, void *p1, void *p2, void *p3) {
  SSnodeObj* pSnode = pObj;
  int32_t* id = (int32_t*)p1;
  SSnodeObj** ppSnode = (SSnodeObj**)p2;

  if (pSnode->id > *id) {
    *id = pSnode->id;
  }

  if (0 == pSnode->replicaId) {
    if (NULL == *ppSnode) {
      *ppSnode = pSnode;
    } else {
      mError("already got no replicaId snode:%d, new no replicaId snode:%d", (*ppSnode)->id, pSnode->id);
    }
  }

  return true;
}


void mndSnodeGetReplicaId(SMnode *pMnode, SMCreateSnodeReq *pCreate, int32_t *replicaId, SSnodeObj** ppSnode) {
  sdbTraverse(pMnode->pSdb, SDB_SNODE, mndSnodeTraverseForCreate, replicaId, ppSnode, NULL);
}

static int32_t mndSnodeAppendUpdateToTrans(SMnode *pMnode, SSnodeObj* pObj, STrans *pTrans, int32_t replicaId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SDnodeObj *pDnode2 = mndAcquireDnode(pMnode, pObj->id);
  if (pDnode2 == NULL) {
    code = TSDB_CODE_MND_DNODE_NOT_EXIST;
    goto _exit;
  }
  
  SSnodeObj snodeObj2 = {0};
  snodeObj2.id = pObj->id;
  snodeObj2.replicaId = replicaId;
  snodeObj2.createdTime = pObj->createdTime;
  snodeObj2.updateTime = taosGetTimestampMs();
  
  mInfo("trans:%d, used to update snode:%d", pTrans->id, snodeObj2.id);
  
  TAOS_CHECK_EXIT(mndSetUpdateSnodeRedoLogs(pTrans, &snodeObj2));
  //TAOS_CHECK_EXIT(mndSetCreateSnodeUndoLogs(pTrans, &snodeObj2));
  TAOS_CHECK_EXIT(mndSetCreateSnodeCommitLogs(pTrans, &snodeObj2));
  TAOS_CHECK_EXIT(mndSetUpdateSnodeRedoActions(pTrans, pDnode2, &snodeObj2, snodeObj2.replicaId));
  //TAOS_CHECK_EXIT(mndSetCreateSnodeUndoActions(pTrans, pDnode2, &snodeObj2));

_exit:

  mndReleaseDnode(pMnode, pDnode2);

  if (code) {
    mError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;  
}

static int32_t mndCreateSnodeWithReplicaId(SMnode *pMnode, SRpcMsg *pReq, SDnodeObj *pDnode, SMCreateSnodeReq *pCreate, int64_t currTs) {
  int32_t replicaId = 0;
  SSnodeObj* noReplicaSnode = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  mndSnodeGetReplicaId(pMnode, pCreate, &replicaId, &noReplicaSnode);

  SSnodeObj snodeObj = {0};
  snodeObj.id = pDnode->id;
  snodeObj.replicaId = replicaId;
  snodeObj.createdTime = currTs;
  snodeObj.updateTime = snodeObj.createdTime;

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
  TAOS_CHECK_EXIT(mndSetCreateSnodeRedoActions(pTrans, pDnode, &snodeObj, snodeObj.replicaId));
  TAOS_CHECK_EXIT(mndSetCreateSnodeUndoActions(pTrans, pDnode, &snodeObj));

  if (noReplicaSnode) {
    TAOS_CHECK_EXIT(mndSnodeAppendUpdateToTrans(pMnode, noReplicaSnode, pTrans, pDnode->id));
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
    goto _OVER;
  } else if (terrno != TSDB_CODE_MND_SNODE_NOT_EXIST) {
    goto _OVER;
  }

  pDnode = mndAcquireDnode(pMnode, createReq.dnodeId);
  if (pDnode == NULL) {
    code = TSDB_CODE_MND_DNODE_NOT_EXIST;
    goto _OVER;
  }

  int32_t replicaId = 0;
  int64_t currTs = taosGetTimestampMs();
  int32_t snodeNum = sdbGetSize(pMnode->pSdb, SDB_SNODE);
  if (snodeNum > 0) {
    code = mndCreateSnodeWithReplicaId(pMnode, pReq, pDnode, &createReq, currTs);
  } else {
    code = mndCreateSnode(pMnode, pReq, pDnode, &createReq, replicaId, currTs);
  }
  
  if (code == 0) {
    code = TSDB_CODE_ACTION_IN_PROGRESS;
    
    MND_STREAM_SET_LAST_TS(STM_EVENT_CREATE_SNODE, currTs);
  }

_OVER:

  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("snode:%d, failed to create since %s", createReq.dnodeId, tstrerror(code));
    TAOS_RETURN(code);
  }

  //  mndReleaseSnode(pMnode, pObj);
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
  TAOS_CHECK_RETURN(mndTransAppendRedolog(pTrans, pRedoRaw));
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
      ctx->affSnode[ctx->affNum++] = pSnode;
    } else {
      mError("%d snodes with same replicaId:%d, %p, %p, %p", ctx->affNum + 1, ctx->target->id, 
          ctx->affSnode[0], ctx->affSnode[1], pSnode);
    }
  }

  return true;
}

static bool mndSnodeTraverseForReplicaNum(SMnode *pMnode, void *pObj, void *p1, void *p2, void *p3) {
  SSnodeObj* pSnode = pObj;
  SSnodeObj* pTarget = (SSnodeObj*)p1;

  if (pSnode == p1) {
    return true;
  }

  if (pSnode->replicaId == pTarget->id) {
    *(int32_t*)p2 += 1;
  }

  return true;
}


static int32_t mndSnodeGetReplicaNum(SMnode *pMnode, void *pObj) {
  int32_t n = 0;
  sdbTraverse(pMnode->pSdb, SDB_SNODE, mndSnodeTraverseForReplicaNum, pObj, &n, NULL);

  return n;
}

static bool mndSnodeTraverseForNewReplica(SMnode *pMnode, void *pObj, void *p1, void *p2, void *p3) {
  SSnodeObj* pSnode = pObj;

  if (pSnode == p1 || pSnode == p2) {
    return true;
  }

  int32_t n = mndSnodeGetReplicaNum(pMnode, pSnode);
  if (0 == n) {
    *(int32_t*)p3 = pSnode->id;
    return false;
  } else if (1 == n && 0 == *(int32_t*)p3) {
    *(int32_t*)p3 = pSnode->id;
  }

  return true;
}


static int32_t mndSnodeCheckDropReplica(SMnode *pMnode, SSnodeObj* pObj, SSnodeObj** ppAff1, int32_t* newRId1, SSnodeObj** ppAff2, int32_t* newRId2) {
  int32_t snodeNum = sdbGetSize(pMnode->pSdb, SDB_SNODE);
  if (0 == pObj->replicaId) {
    mInfo("no need to get drop snode replica, replicaId:%d, remainSnode:%d", pObj->replicaId, snodeNum);
    return TSDB_CODE_SUCCESS;
  }

  if (snodeNum <= 1) {
    mWarn("no need to get drop snode replica, replicaId:%d, remainSnode:%d", pObj->replicaId, snodeNum);
    return TSDB_CODE_SUCCESS;
  }

  SSnodeDropTraversaCtx ctx = {0};
  ctx.target = pObj;

  sdbTraverse(pMnode->pSdb, SDB_SNODE, mndSnodeTraverseForDropAff, &ctx, NULL, NULL);

  if (0 == ctx.affNum) {
    mDebug("no affected snode for dropping snode %d", pObj->id);
    return TSDB_CODE_SUCCESS;
  }

  if (2 == ctx.affNum) {
    *ppAff1 = ctx.affSnode[0];
    *ppAff2 = ctx.affSnode[1];
    *newRId1 = (*ppAff2)->id;
    *newRId2 = (*ppAff1)->id;

    mDebug("two affected snodes for dropping snode %d, first:%d newRId:%d, sencod:%d newRId:%d", 
        pObj->id, ctx.affSnode[0]->id, *newRId1, ctx.affSnode[1]->id, *newRId2);

    return TSDB_CODE_SUCCESS;
  }

  *ppAff1 = ctx.affSnode[0];
  
  if (ctx.affSnode[0]->id != ctx.target->replicaId) {
    *newRId1 = ctx.target->replicaId;

    mDebug("one affected snodes for dropping snode %d, first:%d newRId:%d", pObj->id, ctx.affSnode[0]->id, *newRId1);

    return TSDB_CODE_SUCCESS;
  }

  sdbTraverse(pMnode->pSdb, SDB_SNODE, mndSnodeTraverseForNewReplica, ctx.target, ctx.affSnode[0], newRId1);

  mDebug("one affected snodes for dropping snode %d, first:%d newRId:%d", pObj->id, ctx.affSnode[0]->id, *newRId1);

  return TSDB_CODE_SUCCESS;
}

static int32_t mndDropSnode(SMnode *pMnode, SRpcMsg *pReq, SSnodeObj *pObj) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SSnodeObj* pAffected1 = NULL, *pAffected2 = NULL;
  int32_t newReplicaId1 = 0, newReplicaId2 = 0;
  STrans *pTrans = NULL;
  SArray* streamList = NULL;
  
  TAOS_CHECK_EXIT(msmCheckSnodeReassign(pMnode, pObj, &streamList));
  TAOS_CHECK_EXIT(mndSnodeCheckDropReplica(pMnode, pObj, &pAffected1, &newReplicaId1, &pAffected2, &newReplicaId2));

  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq, "drop-snode");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _exit;
  }
  mndTransSetSerial(pTrans);

  int32_t streamNum = taosArrayGetSize(streamList);
  if (streamNum > 0) {
    for (int32_t i = 0; i < streamNum; ++i) {
      SStreamObj* pStream = taosArrayGetP(streamList, i);
      atomic_store_32(&pStream->mainSnodeId, pObj->replicaId);
      TAOS_CHECK_EXIT(mndStreamTransAppend(pStream, pTrans, SDB_STATUS_READY));
    }
  }

  if (pAffected1) {
    mInfo("trans:%d, used to update snode:%d replica to %d", pTrans->id, pAffected1->id, newReplicaId1);
    TAOS_CHECK_EXIT(mndSnodeAppendUpdateToTrans(pMnode, pAffected1, pTrans, newReplicaId1));
  }

  if (pAffected2) {
    mInfo("trans:%d, used to update snode:%d replica to %d", pTrans->id, pAffected2->id, newReplicaId2);
    TAOS_CHECK_EXIT(mndSnodeAppendUpdateToTrans(pMnode, pAffected2, pTrans, newReplicaId2));
  }

  mInfo("trans:%d, used to drop snode:%d", pTrans->id, pObj->id);
  TAOS_CHECK_GOTO(mndSetDropSnodeInfoToTrans(pMnode, pTrans, pObj, false), NULL, _exit);
  
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _exit);

_exit:

  mndTransDrop(pTrans);
  taosArrayDestroy(streamList);

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
