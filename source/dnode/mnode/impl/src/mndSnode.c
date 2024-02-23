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
#include "mndSnode.h"
#include "mndDnode.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndTrans.h"
#include "mndUser.h"

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
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_SNODE, SNODE_VER_NUMBER, sizeof(SSnodeObj) + SNODE_RESERVE_SIZE);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pObj->id, _OVER)
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
  pOld->updateTime = pNew->updateTime;
  return 0;
}

static int32_t mndSetCreateSnodeRedoLogs(STrans *pTrans, SSnodeObj *pObj) {
  SSdbRaw *pRedoRaw = mndSnodeActionEncode(pObj);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING) != 0) return -1;
  return 0;
}

static int32_t mndSetCreateSnodeUndoLogs(STrans *pTrans, SSnodeObj *pObj) {
  SSdbRaw *pUndoRaw = mndSnodeActionEncode(pObj);
  if (pUndoRaw == NULL) return -1;
  if (mndTransAppendUndolog(pTrans, pUndoRaw) != 0) return -1;
  if (sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED) != 0) return -1;
  return 0;
}

static int32_t mndSetCreateSnodeCommitLogs(STrans *pTrans, SSnodeObj *pObj) {
  SSdbRaw *pCommitRaw = mndSnodeActionEncode(pObj);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY) != 0) return -1;
  return 0;
}

static int32_t mndSetCreateSnodeRedoActions(STrans *pTrans, SDnodeObj *pDnode, SSnodeObj *pObj) {
  SDCreateSnodeReq createReq = {0};
  createReq.dnodeId = pDnode->id;

  int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &createReq);
  void   *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  tSerializeSCreateDropMQSNodeReq(pReq, contLen, &createReq);

  STransAction action = {0};
  action.epSet = mndGetDnodeEpset(pDnode);
  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_CREATE_SNODE;
  action.acceptableCode = TSDB_CODE_SNODE_ALREADY_DEPLOYED;

  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }

  return 0;
}

static int32_t mndSetCreateSnodeUndoActions(STrans *pTrans, SDnodeObj *pDnode, SSnodeObj *pObj) {
  SDDropSnodeReq dropReq = {0};
  dropReq.dnodeId = pDnode->id;

  int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &dropReq);
  void   *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  tSerializeSCreateDropMQSNodeReq(pReq, contLen, &dropReq);

  STransAction action = {0};
  action.epSet = mndGetDnodeEpset(pDnode);
  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_DROP_SNODE;
  action.acceptableCode = TSDB_CODE_SNODE_NOT_DEPLOYED;

  if (mndTransAppendUndoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }

  return 0;
}

static int32_t mndCreateSnode(SMnode *pMnode, SRpcMsg *pReq, SDnodeObj *pDnode, SMCreateSnodeReq *pCreate) {
  int32_t code = -1;

  SSnodeObj snodeObj = {0};
  snodeObj.id = pDnode->id;
  snodeObj.createdTime = taosGetTimestampMs();
  snodeObj.updateTime = snodeObj.createdTime;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "create-snode");
  if (pTrans == NULL) goto _OVER;
  mndTransSetSerial(pTrans);

  mInfo("trans:%d, used to create snode:%d", pTrans->id, pCreate->dnodeId);

  if (mndSetCreateSnodeRedoLogs(pTrans, &snodeObj) != 0) goto _OVER;
  if (mndSetCreateSnodeUndoLogs(pTrans, &snodeObj) != 0) goto _OVER;
  if (mndSetCreateSnodeCommitLogs(pTrans, &snodeObj) != 0) goto _OVER;
  if (mndSetCreateSnodeRedoActions(pTrans, pDnode, &snodeObj) != 0) goto _OVER;
  if (mndSetCreateSnodeUndoActions(pTrans, pDnode, &snodeObj) != 0) goto _OVER;
  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessCreateSnodeReq(SRpcMsg *pReq) {
  SMnode          *pMnode = pReq->info.node;
  int32_t          code = -1;
  SSnodeObj       *pObj = NULL;
  SDnodeObj       *pDnode = NULL;
  SMCreateSnodeReq createReq = {0};

  if (tDeserializeSCreateDropMQSNodeReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("snode:%d, start to create", createReq.dnodeId);
  if (mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CREATE_SNODE) != 0) {
    goto _OVER;
  }

//  pObj = mndAcquireSnode(pMnode, createReq.dnodeId);
//  if (pObj != NULL) {
//    terrno = TSDB_CODE_MND_SNODE_ALREADY_EXIST;
//    goto _OVER;
//  } else if (terrno != TSDB_CODE_MND_SNODE_NOT_EXIST) {
//    goto _OVER;
//  }

  if (sdbGetSize(pMnode->pSdb, SDB_SNODE) >= 1){
    terrno = TSDB_CODE_MND_SNODE_ALREADY_EXIST;
    goto _OVER;
  }

  pDnode = mndAcquireDnode(pMnode, createReq.dnodeId);
  if (pDnode == NULL) {
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    goto _OVER;
  }

  code = mndCreateSnode(pMnode, pReq, pDnode, &createReq);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("snode:%d, failed to create since %s", createReq.dnodeId, terrstr());
    return -1;
  }

//  mndReleaseSnode(pMnode, pObj);
  mndReleaseDnode(pMnode, pDnode);
  tFreeSMCreateQnodeReq(&createReq);
  return code;
}

static int32_t mndSetDropSnodeRedoLogs(STrans *pTrans, SSnodeObj *pObj) {
  SSdbRaw *pRedoRaw = mndSnodeActionEncode(pObj);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING) != 0) return -1;
  return 0;
}

static int32_t mndSetDropSnodeCommitLogs(STrans *pTrans, SSnodeObj *pObj) {
  SSdbRaw *pCommitRaw = mndSnodeActionEncode(pObj);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED) != 0) return -1;
  return 0;
}

static int32_t mndSetDropSnodeRedoActions(STrans *pTrans, SDnodeObj *pDnode, SSnodeObj *pObj) {
  SDDropSnodeReq dropReq = {0};
  dropReq.dnodeId = pDnode->id;

  int32_t contLen = tSerializeSCreateDropMQSNodeReq(NULL, 0, &dropReq);
  void   *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  tSerializeSCreateDropMQSNodeReq(pReq, contLen, &dropReq);

  STransAction action = {0};
  action.epSet = mndGetDnodeEpset(pDnode);
  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_DROP_SNODE;
  action.acceptableCode = TSDB_CODE_SNODE_NOT_DEPLOYED;

  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }

  return 0;
}

int32_t mndSetDropSnodeInfoToTrans(SMnode *pMnode, STrans *pTrans, SSnodeObj *pObj, bool force) {
  if (pObj == NULL) return 0;
  if (mndSetDropSnodeRedoLogs(pTrans, pObj) != 0) return -1;
  if (mndSetDropSnodeCommitLogs(pTrans, pObj) != 0) return -1;
  if (!force) {
    if (mndSetDropSnodeRedoActions(pTrans, pObj->pDnode, pObj) != 0) return -1;
  }
  return 0;
}

static int32_t mndDropSnode(SMnode *pMnode, SRpcMsg *pReq, SSnodeObj *pObj) {
  int32_t code = -1;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq, "drop-snode");
  if (pTrans == NULL) goto _OVER;
  mndTransSetSerial(pTrans);

  mInfo("trans:%d, used to drop snode:%d", pTrans->id, pObj->id);
  if (mndSetDropSnodeInfoToTrans(pMnode, pTrans, pObj, false) != 0) goto _OVER;
  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessDropSnodeReq(SRpcMsg *pReq) {
  SMnode        *pMnode = pReq->info.node;
  int32_t        code = -1;
  SSnodeObj     *pObj = NULL;
  SMDropSnodeReq dropReq = {0};

  if (tDeserializeSCreateDropMQSNodeReq(pReq->pCont, pReq->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("snode:%d, start to drop", dropReq.dnodeId);
  if (mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_DROP_SNODE) != 0) {
    goto _OVER;
  }

  if (dropReq.dnodeId <= 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  pObj = mndAcquireSnode(pMnode, dropReq.dnodeId);
  if (pObj == NULL) {
    goto _OVER;
  }

  // check deletable
  code = mndDropSnode(pMnode, pReq, pObj);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("snode:%d, failed to drop since %s", dropReq.dnodeId, terrstr());
  }

  mndReleaseSnode(pMnode, pObj);
  tFreeSMCreateQnodeReq(&dropReq);
  return code;
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
    colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->id, false);

    char ep[TSDB_EP_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(ep, pObj->pDnode->ep, pShow->pMeta->pSchemas[cols].bytes);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)ep, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->createdTime, false);

    numOfRows++;
    sdbRelease(pSdb, pObj);
  }

  pShow->numOfRows += numOfRows;

  return numOfRows;
}

static void mndCancelGetNextSnode(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}
