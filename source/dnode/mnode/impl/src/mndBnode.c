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
#include "mndBnode.h"
#include "mndAuth.h"
#include "mndDnode.h"
#include "mndShow.h"
#include "mndTrans.h"
#include "mndUser.h"

#define BNODE_VER_NUMBER   1
#define BNODE_RESERVE_SIZE 64

static SSdbRaw *mndBnodeActionEncode(SBnodeObj *pObj);
static SSdbRow *mndBnodeActionDecode(SSdbRaw *pRaw);
static int32_t  mndBnodeActionInsert(SSdb *pSdb, SBnodeObj *pObj);
static int32_t  mndBnodeActionUpdate(SSdb *pSdb, SBnodeObj *pOld, SBnodeObj *pNew);
static int32_t  mndBnodeActionDelete(SSdb *pSdb, SBnodeObj *pObj);
static int32_t  mndProcessCreateBnodeReq(SNodeMsg *pReq);
static int32_t  mndProcessCreateBnodeRsp(SNodeMsg *pRsp);
static int32_t  mndProcessDropBnodeReq(SNodeMsg *pReq);
static int32_t  mndProcessDropBnodeRsp(SNodeMsg *pRsp);
static int32_t  mndGetBnodeMeta(SNodeMsg *pReq, SShowObj *pShow, STableMetaRsp *pMeta);
static int32_t  mndRetrieveBnodes(SNodeMsg *pReq, SShowObj *pShow, char *data, int32_t rows);
static void     mndCancelGetNextBnode(SMnode *pMnode, void *pIter);

int32_t mndInitBnode(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_BNODE,
                     .keyType = SDB_KEY_INT32,
                     .encodeFp = (SdbEncodeFp)mndBnodeActionEncode,
                     .decodeFp = (SdbDecodeFp)mndBnodeActionDecode,
                     .insertFp = (SdbInsertFp)mndBnodeActionInsert,
                     .updateFp = (SdbUpdateFp)mndBnodeActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndBnodeActionDelete};

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_BNODE, mndProcessCreateBnodeReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_BNODE, mndProcessDropBnodeReq);
  mndSetMsgHandle(pMnode, TDMT_DND_CREATE_BNODE_RSP, mndProcessCreateBnodeRsp);
  mndSetMsgHandle(pMnode, TDMT_DND_DROP_BNODE_RSP, mndProcessDropBnodeRsp);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_BNODE, mndRetrieveBnodes);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_BNODE, mndCancelGetNextBnode);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupBnode(SMnode *pMnode) {}

static SBnodeObj *mndAcquireBnode(SMnode *pMnode, int32_t bnodeId) {
  SBnodeObj *pObj = sdbAcquire(pMnode->pSdb, SDB_BNODE, &bnodeId);
  if (pObj == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_MND_BNODE_NOT_EXIST;
  }
  return pObj;
}

static void mndReleaseBnode(SMnode *pMnode, SBnodeObj *pObj) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pObj);
}

static SSdbRaw *mndBnodeActionEncode(SBnodeObj *pObj) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_BNODE, BNODE_VER_NUMBER, sizeof(SBnodeObj) + BNODE_RESERVE_SIZE);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pObj->id, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->createdTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->updateTime, _OVER)
  SDB_SET_RESERVE(pRaw, dataPos, BNODE_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("bnode:%d, failed to encode to raw:%p since %s", pObj->id, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("bnode:%d, encode to raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRaw;
}

static SSdbRow *mndBnodeActionDecode(SSdbRaw *pRaw) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver != BNODE_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  SSdbRow *pRow = sdbAllocRow(sizeof(SBnodeObj));
  if (pRow == NULL) goto _OVER;

  SBnodeObj *pObj = sdbGetRowObj(pRow);
  if (pObj == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &pObj->id, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->createdTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->updateTime, _OVER)
  SDB_GET_RESERVE(pRaw, dataPos, BNODE_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("bnode:%d, failed to decode from raw:%p since %s", pObj->id, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("bnode:%d, decode from raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRow;
}

static int32_t mndBnodeActionInsert(SSdb *pSdb, SBnodeObj *pObj) {
  mTrace("bnode:%d, perform insert action, row:%p", pObj->id, pObj);
  pObj->pDnode = sdbAcquire(pSdb, SDB_DNODE, &pObj->id);
  if (pObj->pDnode == NULL) {
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    mError("bnode:%d, failed to perform insert action since %s", pObj->id, terrstr());
    return -1;
  }

  return 0;
}

static int32_t mndBnodeActionDelete(SSdb *pSdb, SBnodeObj *pObj) {
  mTrace("bnode:%d, perform delete action, row:%p", pObj->id, pObj);
  if (pObj->pDnode != NULL) {
    sdbRelease(pSdb, pObj->pDnode);
    pObj->pDnode = NULL;
  }

  return 0;
}

static int32_t mndBnodeActionUpdate(SSdb *pSdb, SBnodeObj *pOld, SBnodeObj *pNew) {
  mTrace("bnode:%d, perform update action, old row:%p new row:%p", pOld->id, pOld, pNew);
  pOld->updateTime = pNew->updateTime;
  return 0;
}

static int32_t mndSetCreateBnodeRedoLogs(STrans *pTrans, SBnodeObj *pObj) {
  SSdbRaw *pRedoRaw = mndBnodeActionEncode(pObj);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING) != 0) return -1;
  return 0;
}

static int32_t mndSetCreateBnodeUndoLogs(STrans *pTrans, SBnodeObj *pObj) {
  SSdbRaw *pUndoRaw = mndBnodeActionEncode(pObj);
  if (pUndoRaw == NULL) return -1;
  if (mndTransAppendUndolog(pTrans, pUndoRaw) != 0) return -1;
  if (sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED) != 0) return -1;
  return 0;
}

static int32_t mndSetCreateBnodeCommitLogs(STrans *pTrans, SBnodeObj *pObj) {
  SSdbRaw *pCommitRaw = mndBnodeActionEncode(pObj);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY) != 0) return -1;
  return 0;
}

static int32_t mndSetCreateBnodeRedoActions(STrans *pTrans, SDnodeObj *pDnode, SBnodeObj *pObj) {
  SDCreateBnodeReq createReq = {0};
  createReq.dnodeId = pDnode->id;

  int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &createReq);
  void   *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &createReq);

  STransAction action = {0};
  action.epSet = mndGetDnodeEpset(pDnode);
  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_CREATE_BNODE;
  action.acceptableCode = TSDB_CODE_NODE_ALREADY_DEPLOYED;

  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }

  return 0;
}

static int32_t mndSetCreateBnodeUndoActions(STrans *pTrans, SDnodeObj *pDnode, SBnodeObj *pObj) {
  SDDropBnodeReq dropReq = {0};
  dropReq.dnodeId = pDnode->id;

  int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &dropReq);
  void   *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &dropReq);

  STransAction action = {0};
  action.epSet = mndGetDnodeEpset(pDnode);
  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_DROP_BNODE;
  action.acceptableCode = TSDB_CODE_NODE_NOT_DEPLOYED;

  if (mndTransAppendUndoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }

  return 0;
}

static int32_t mndCreateBnode(SMnode *pMnode, SNodeMsg *pReq, SDnodeObj *pDnode, SMCreateBnodeReq *pCreate) {
  int32_t code = -1;

  SBnodeObj bnodeObj = {0};
  bnodeObj.id = pDnode->id;
  bnodeObj.createdTime = taosGetTimestampMs();
  bnodeObj.updateTime = bnodeObj.createdTime;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_TYPE_CREATE_BNODE, &pReq->rpcMsg);
  if (pTrans == NULL) goto _OVER;

  mDebug("trans:%d, used to create bnode:%d", pTrans->id, pCreate->dnodeId);
  if (mndSetCreateBnodeRedoLogs(pTrans, &bnodeObj) != 0) goto _OVER;
  if (mndSetCreateBnodeUndoLogs(pTrans, &bnodeObj) != 0) goto _OVER;
  if (mndSetCreateBnodeCommitLogs(pTrans, &bnodeObj) != 0) goto _OVER;
  if (mndSetCreateBnodeRedoActions(pTrans, pDnode, &bnodeObj) != 0) goto _OVER;
  if (mndSetCreateBnodeUndoActions(pTrans, pDnode, &bnodeObj) != 0) goto _OVER;
  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessCreateBnodeReq(SNodeMsg *pReq) {
  SMnode          *pMnode = pReq->pNode;
  int32_t          code = -1;
  SBnodeObj       *pObj = NULL;
  SDnodeObj       *pDnode = NULL;
  SUserObj        *pUser = NULL;
  SMCreateBnodeReq createReq = {0};

  if (tDeserializeSCreateDropMQSBNodeReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mDebug("bnode:%d, start to create", createReq.dnodeId);

  pObj = mndAcquireBnode(pMnode, createReq.dnodeId);
  if (pObj != NULL) {
    terrno = TSDB_CODE_MND_BNODE_ALREADY_EXIST;
    goto _OVER;
  } else if (terrno != TSDB_CODE_MND_BNODE_NOT_EXIST) {
    goto _OVER;
  }

  pDnode = mndAcquireDnode(pMnode, createReq.dnodeId);
  if (pDnode == NULL) {
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    goto _OVER;
  }

  pUser = mndAcquireUser(pMnode, pReq->user);
  if (pUser == NULL) {
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
    goto _OVER;
  }

  if (mndCheckNodeAuth(pUser)) {
    goto _OVER;
  }

  code = mndCreateBnode(pMnode, pReq, pDnode, &createReq);
  if (code == 0) code = TSDB_CODE_MND_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("bnode:%d, failed to create since %s", createReq.dnodeId, terrstr());
  }

  mndReleaseBnode(pMnode, pObj);
  mndReleaseDnode(pMnode, pDnode);
  mndReleaseUser(pMnode, pUser);
  return code;
}

static int32_t mndSetDropBnodeRedoLogs(STrans *pTrans, SBnodeObj *pObj) {
  SSdbRaw *pRedoRaw = mndBnodeActionEncode(pObj);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING) != 0) return -1;
  return 0;
}

static int32_t mndSetDropBnodeCommitLogs(STrans *pTrans, SBnodeObj *pObj) {
  SSdbRaw *pCommitRaw = mndBnodeActionEncode(pObj);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED) != 0) return -1;
  return 0;
}

static int32_t mndSetDropBnodeRedoActions(STrans *pTrans, SDnodeObj *pDnode, SBnodeObj *pObj) {
  SDDropBnodeReq dropReq = {0};
  dropReq.dnodeId = pDnode->id;

  int32_t contLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &dropReq);
  void   *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  tSerializeSCreateDropMQSBNodeReq(pReq, contLen, &dropReq);

  STransAction action = {0};
  action.epSet = mndGetDnodeEpset(pDnode);
  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_DROP_BNODE;
  action.acceptableCode = TSDB_CODE_NODE_NOT_DEPLOYED;

  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }

  return 0;
}

static int32_t mndDropBnode(SMnode *pMnode, SNodeMsg *pReq, SBnodeObj *pObj) {
  int32_t code = -1;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_TYPE_DROP_BNODE, &pReq->rpcMsg);
  if (pTrans == NULL) goto _OVER;

  mDebug("trans:%d, used to drop bnode:%d", pTrans->id, pObj->id);
  if (mndSetDropBnodeRedoLogs(pTrans, pObj) != 0) goto _OVER;
  if (mndSetDropBnodeCommitLogs(pTrans, pObj) != 0) goto _OVER;
  if (mndSetDropBnodeRedoActions(pTrans, pObj->pDnode, pObj) != 0) goto _OVER;
  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessDropBnodeReq(SNodeMsg *pReq) {
  SMnode        *pMnode = pReq->pNode;
  int32_t        code = -1;
  SUserObj      *pUser = NULL;
  SBnodeObj     *pObj = NULL;
  SMDropBnodeReq dropReq = {0};

  if (tDeserializeSCreateDropMQSBNodeReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mDebug("bnode:%d, start to drop", dropReq.dnodeId);

  if (dropReq.dnodeId <= 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  pObj = mndAcquireBnode(pMnode, dropReq.dnodeId);
  if (pObj == NULL) {
    goto _OVER;
  }

  pUser = mndAcquireUser(pMnode, pReq->user);
  if (pUser == NULL) {
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
    goto _OVER;
  }

  if (mndCheckNodeAuth(pUser)) {
    goto _OVER;
  }

  code = mndDropBnode(pMnode, pReq, pObj);
  if (code == 0) code = TSDB_CODE_MND_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("bnode:%d, failed to drop since %s", dropReq.dnodeId, terrstr());
  }

  mndReleaseBnode(pMnode, pObj);
  mndReleaseUser(pMnode, pUser);

  return code;
}

static int32_t mndProcessCreateBnodeRsp(SNodeMsg *pRsp) {
  mndTransProcessRsp(pRsp);
  return 0;
}

static int32_t mndProcessDropBnodeRsp(SNodeMsg *pRsp) {
  mndTransProcessRsp(pRsp);
  return 0;
}

static int32_t mndRetrieveBnodes(SNodeMsg *pReq, SShowObj *pShow, char *data, int32_t rows) {
  SMnode    *pMnode = pReq->pNode;
  SSdb      *pSdb = pMnode->pSdb;
  int32_t    numOfRows = 0;
  int32_t    cols = 0;
  SBnodeObj *pObj = NULL;
  char      *pWrite;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_BNODE, pShow->pIter, (void **)&pObj);
    if (pShow->pIter == NULL) break;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pObj->id;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pObj->pDnode->ep, pShow->bytes[cols]);

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

static void mndCancelGetNextBnode(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}
