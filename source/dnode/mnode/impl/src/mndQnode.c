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
#include "mndQnode.h"
#include "mndAuth.h"
#include "mndDnode.h"
#include "mndShow.h"
#include "mndTrans.h"
#include "mndUser.h"

#define QNODE_VER_NUMBER   1
#define QNODE_RESERVE_SIZE 64

static SSdbRaw *mndQnodeActionEncode(SQnodeObj *pObj);
static SSdbRow *mndQnodeActionDecode(SSdbRaw *pRaw);
static int32_t  mndQnodeActionInsert(SSdb *pSdb, SQnodeObj *pObj);
static int32_t  mndQnodeActionUpdate(SSdb *pSdb, SQnodeObj *pOld, SQnodeObj *pNew);
static int32_t  mndQnodeActionDelete(SSdb *pSdb, SQnodeObj *pObj);
static int32_t  mndProcessCreateQnodeReq(SNodeMsg *pReq);
static int32_t  mndProcessCreateQnodeRsp(SNodeMsg *pRsp);
static int32_t  mndProcessDropQnodeReq(SNodeMsg *pReq);
static int32_t  mndProcessDropQnodeRsp(SNodeMsg *pRsp);
static int32_t  mndProcessQnodeListReq(SNodeMsg *pReq);
static int32_t  mndRetrieveQnodes(SNodeMsg *pReq, SShowObj *pShow, char *data, int32_t rows);
static void     mndCancelGetNextQnode(SMnode *pMnode, void *pIter);

int32_t mndInitQnode(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_QNODE,
                     .keyType = SDB_KEY_INT32,
                     .encodeFp = (SdbEncodeFp)mndQnodeActionEncode,
                     .decodeFp = (SdbDecodeFp)mndQnodeActionDecode,
                     .insertFp = (SdbInsertFp)mndQnodeActionInsert,
                     .updateFp = (SdbUpdateFp)mndQnodeActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndQnodeActionDelete};

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_QNODE, mndProcessCreateQnodeReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_QNODE, mndProcessDropQnodeReq);
  mndSetMsgHandle(pMnode, TDMT_DND_CREATE_QNODE_RSP, mndProcessCreateQnodeRsp);
  mndSetMsgHandle(pMnode, TDMT_DND_DROP_QNODE_RSP, mndProcessDropQnodeRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_QNODE_LIST, mndProcessQnodeListReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_QNODE, mndRetrieveQnodes);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_QNODE, mndCancelGetNextQnode);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupQnode(SMnode *pMnode) {}

static SQnodeObj *mndAcquireQnode(SMnode *pMnode, int32_t qnodeId) {
  SQnodeObj *pObj = sdbAcquire(pMnode->pSdb, SDB_QNODE, &qnodeId);
  if (pObj == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_MND_QNODE_NOT_EXIST;
  }
  return pObj;
}

static void mndReleaseQnode(SMnode *pMnode, SQnodeObj *pObj) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pObj);
}

static SSdbRaw *mndQnodeActionEncode(SQnodeObj *pObj) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_QNODE, QNODE_VER_NUMBER, sizeof(SQnodeObj) + QNODE_RESERVE_SIZE);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pObj->id, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->createdTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->updateTime, _OVER)
  SDB_SET_RESERVE(pRaw, dataPos, QNODE_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("qnode:%d, failed to encode to raw:%p since %s", pObj->id, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("qnode:%d, encode to raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRaw;
}

static SSdbRow *mndQnodeActionDecode(SSdbRaw *pRaw) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver != QNODE_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  SSdbRow *pRow = sdbAllocRow(sizeof(SQnodeObj));
  if (pRow == NULL) goto _OVER;

  SQnodeObj *pObj = sdbGetRowObj(pRow);
  if (pObj == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &pObj->id, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->createdTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->updateTime, _OVER)
  SDB_GET_RESERVE(pRaw, dataPos, QNODE_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("qnode:%d, failed to decode from raw:%p since %s", pObj->id, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("qnode:%d, decode from raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRow;
}

static int32_t mndQnodeActionInsert(SSdb *pSdb, SQnodeObj *pObj) {
  mTrace("qnode:%d, perform insert action, row:%p", pObj->id, pObj);
  pObj->pDnode = sdbAcquire(pSdb, SDB_DNODE, &pObj->id);
  if (pObj->pDnode == NULL) {
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    mError("qnode:%d, failed to perform insert action since %s", pObj->id, terrstr());
    return -1;
  }

  return 0;
}

static int32_t mndQnodeActionDelete(SSdb *pSdb, SQnodeObj *pObj) {
  mTrace("qnode:%d, perform delete action, row:%p", pObj->id, pObj);
  if (pObj->pDnode != NULL) {
    sdbRelease(pSdb, pObj->pDnode);
    pObj->pDnode = NULL;
  }

  return 0;
}

static int32_t mndQnodeActionUpdate(SSdb *pSdb, SQnodeObj *pOld, SQnodeObj *pNew) {
  mTrace("qnode:%d, perform update action, old row:%p new row:%p", pOld->id, pOld, pNew);
  pOld->updateTime = pNew->updateTime;
  return 0;
}

static int32_t mndSetCreateQnodeRedoLogs(STrans *pTrans, SQnodeObj *pObj) {
  SSdbRaw *pRedoRaw = mndQnodeActionEncode(pObj);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING) != 0) return -1;
  return 0;
}

static int32_t mndSetCreateQnodeUndoLogs(STrans *pTrans, SQnodeObj *pObj) {
  SSdbRaw *pUndoRaw = mndQnodeActionEncode(pObj);
  if (pUndoRaw == NULL) return -1;
  if (mndTransAppendUndolog(pTrans, pUndoRaw) != 0) return -1;
  if (sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED) != 0) return -1;
  return 0;
}

static int32_t mndSetCreateQnodeCommitLogs(STrans *pTrans, SQnodeObj *pObj) {
  SSdbRaw *pCommitRaw = mndQnodeActionEncode(pObj);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY) != 0) return -1;
  return 0;
}

static int32_t mndSetCreateQnodeRedoActions(STrans *pTrans, SDnodeObj *pDnode, SQnodeObj *pObj) {
  SDCreateQnodeReq createReq = {0};
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
  action.msgType = TDMT_DND_CREATE_QNODE;
  action.acceptableCode = TSDB_CODE_NODE_ALREADY_DEPLOYED;

  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }

  return 0;
}

static int32_t mndSetCreateQnodeUndoActions(STrans *pTrans, SDnodeObj *pDnode, SQnodeObj *pObj) {
  SDDropQnodeReq dropReq = {0};
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
  action.msgType = TDMT_DND_DROP_QNODE;
  action.acceptableCode = TSDB_CODE_NODE_NOT_DEPLOYED;

  if (mndTransAppendUndoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }

  return 0;
}

static int32_t mndCreateQnode(SMnode *pMnode, SNodeMsg *pReq, SDnodeObj *pDnode, SMCreateQnodeReq *pCreate) {
  int32_t code = -1;

  SQnodeObj qnodeObj = {0};
  qnodeObj.id = pDnode->id;
  qnodeObj.createdTime = taosGetTimestampMs();
  qnodeObj.updateTime = qnodeObj.createdTime;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_TYPE_CREATE_QNODE, &pReq->rpcMsg);
  if (pTrans == NULL) goto _OVER;

  mDebug("trans:%d, used to create qnode:%d", pTrans->id, pCreate->dnodeId);
  if (mndSetCreateQnodeRedoLogs(pTrans, &qnodeObj) != 0) goto _OVER;
  if (mndSetCreateQnodeUndoLogs(pTrans, &qnodeObj) != 0) goto _OVER;
  if (mndSetCreateQnodeCommitLogs(pTrans, &qnodeObj) != 0) goto _OVER;
  if (mndSetCreateQnodeRedoActions(pTrans, pDnode, &qnodeObj) != 0) goto _OVER;
  if (mndSetCreateQnodeUndoActions(pTrans, pDnode, &qnodeObj) != 0) goto _OVER;
  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessCreateQnodeReq(SNodeMsg *pReq) {
  SMnode          *pMnode = pReq->pNode;
  int32_t          code = -1;
  SQnodeObj       *pObj = NULL;
  SDnodeObj       *pDnode = NULL;
  SUserObj        *pUser = NULL;
  SMCreateQnodeReq createReq = {0};

  if (tDeserializeSCreateDropMQSBNodeReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mDebug("qnode:%d, start to create", createReq.dnodeId);

  pObj = mndAcquireQnode(pMnode, createReq.dnodeId);
  if (pObj != NULL) {
    terrno = TSDB_CODE_MND_QNODE_ALREADY_EXIST;
    goto _OVER;
  } else if (terrno != TSDB_CODE_MND_QNODE_NOT_EXIST) {
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

  code = mndCreateQnode(pMnode, pReq, pDnode, &createReq);
  if (code == 0) code = TSDB_CODE_MND_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("qnode:%d, failed to create since %s", createReq.dnodeId, terrstr());
  }

  mndReleaseQnode(pMnode, pObj);
  mndReleaseDnode(pMnode, pDnode);
  mndReleaseUser(pMnode, pUser);
  return code;
}

static int32_t mndSetDropQnodeRedoLogs(STrans *pTrans, SQnodeObj *pObj) {
  SSdbRaw *pRedoRaw = mndQnodeActionEncode(pObj);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING) != 0) return -1;
  return 0;
}

static int32_t mndSetDropQnodeCommitLogs(STrans *pTrans, SQnodeObj *pObj) {
  SSdbRaw *pCommitRaw = mndQnodeActionEncode(pObj);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED) != 0) return -1;
  return 0;
}

static int32_t mndSetDropQnodeRedoActions(STrans *pTrans, SDnodeObj *pDnode, SQnodeObj *pObj) {
  SDDropQnodeReq dropReq = {0};
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
  action.msgType = TDMT_DND_DROP_QNODE;
  action.acceptableCode = TSDB_CODE_NODE_NOT_DEPLOYED;

  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }

  return 0;
}

static int32_t mndDropQnode(SMnode *pMnode, SNodeMsg *pReq, SQnodeObj *pObj) {
  int32_t code = -1;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_TYPE_DROP_QNODE, &pReq->rpcMsg);
  if (pTrans == NULL) goto _OVER;

  mDebug("trans:%d, used to drop qnode:%d", pTrans->id, pObj->id);
  if (mndSetDropQnodeRedoLogs(pTrans, pObj) != 0) goto _OVER;
  if (mndSetDropQnodeCommitLogs(pTrans, pObj) != 0) goto _OVER;
  if (mndSetDropQnodeRedoActions(pTrans, pObj->pDnode, pObj) != 0) goto _OVER;
  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessDropQnodeReq(SNodeMsg *pReq) {
  SMnode        *pMnode = pReq->pNode;
  int32_t        code = -1;
  SUserObj      *pUser = NULL;
  SQnodeObj     *pObj = NULL;
  SMDropQnodeReq dropReq = {0};

  if (tDeserializeSCreateDropMQSBNodeReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mDebug("qnode:%d, start to drop", dropReq.dnodeId);

  if (dropReq.dnodeId <= 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  pObj = mndAcquireQnode(pMnode, dropReq.dnodeId);
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

  code = mndDropQnode(pMnode, pReq, pObj);
  if (code == 0) code = TSDB_CODE_MND_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("qnode:%d, failed to drop since %s", dropReq.dnodeId, terrstr());
  }

  mndReleaseQnode(pMnode, pObj);
  mndReleaseUser(pMnode, pUser);

  return code;
}

static int32_t mndProcessQnodeListReq(SNodeMsg *pReq) {
  int32_t       code = -1;
  int32_t       numOfRows = 0;
  SMnode       *pMnode = pReq->pNode;
  SSdb         *pSdb = pMnode->pSdb;
  SQnodeObj    *pObj = NULL;
  SQnodeListReq qlistReq = {0};
  SQnodeListRsp qlistRsp = {0};

  if (tDeserializeSQnodeListReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &qlistReq) != 0) {
    mError("failed to parse qnode list req");
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  qlistRsp.epSetList = taosArrayInit(5, sizeof(SEpSet));
  if (NULL == qlistRsp.epSetList) {
    mError("failed to alloc epSet while process qnode list req");
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  while (1) {
    void *pIter = sdbFetch(pSdb, SDB_QNODE, NULL, (void **)&pObj);
    if (pIter == NULL) break;

    SEpSet epSet = {.numOfEps = 1};
    tstrncpy(epSet.eps[0].fqdn, pObj->pDnode->fqdn, TSDB_FQDN_LEN);
    epSet.eps[0].port = pObj->pDnode->port;

    (void)taosArrayPush(qlistRsp.epSetList, &epSet);

    numOfRows++;
    sdbRelease(pSdb, pObj);

    if (qlistReq.rowNum > 0 && numOfRows >= qlistReq.rowNum) {
      break;
    }
  }

  int32_t rspLen = tSerializeSQnodeListRsp(NULL, 0, &qlistRsp);
  void   *pRsp = taosMemoryMalloc(rspLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  tSerializeSQnodeListRsp(pRsp, rspLen, &qlistRsp);

  pReq->rspLen = rspLen;
  pReq->pRsp = pRsp;
  code = 0;

_OVER:
  tFreeSQnodeListRsp(&qlistRsp);
  return code;
}

static int32_t mndProcessCreateQnodeRsp(SNodeMsg *pRsp) {
  mndTransProcessRsp(pRsp);
  return 0;
}

static int32_t mndProcessDropQnodeRsp(SNodeMsg *pRsp) {
  mndTransProcessRsp(pRsp);
  return 0;
}

static int32_t mndRetrieveQnodes(SNodeMsg *pReq, SShowObj *pShow, char *data, int32_t rows) {
  SMnode    *pMnode = pReq->pNode;
  SSdb      *pSdb = pMnode->pSdb;
  int32_t    numOfRows = 0;
  int32_t    cols = 0;
  SQnodeObj *pObj = NULL;
  char      *pWrite;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_QNODE, pShow->pIter, (void **)&pObj);
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

static void mndCancelGetNextQnode(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}
