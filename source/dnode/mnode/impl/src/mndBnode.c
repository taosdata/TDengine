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
#include "mndDnode.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndTrans.h"
#include "mndUser.h"

#define BNODE_VER_NUMBER   1
#define BNODE_RESERVE_SIZE 64

static SSdbRaw *mndBnodeActionEncode(SBnodeObj *pObj) {
  int32_t code = 0;
  int32_t lino = 0;

  terrno = TSDB_CODE_OUT_OF_MEMORY;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_BNODE, BNODE_VER_NUMBER, sizeof(SBnodeObj) + BNODE_RESERVE_SIZE);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pObj->id, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->proto, _OVER)
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
  int32_t    code = 0;
  int32_t    lino = 0;
  SSdbRow   *pRow = NULL;
  SBnodeObj *pObj = NULL;
  int8_t     sver = 0;

  terrno = TSDB_CODE_OUT_OF_MEMORY;

  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver != BNODE_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  pRow = sdbAllocRow(sizeof(SBnodeObj));
  if (pRow == NULL) goto _OVER;

  pObj = sdbGetRowObj(pRow);
  if (pObj == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &pObj->id, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pObj->proto, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->createdTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->updateTime, _OVER)
  SDB_GET_RESERVE(pRaw, dataPos, BNODE_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("bnode:%d, failed to decode from raw:%p since %s", pObj == NULL ? 0 : pObj->id, pRaw, terrstr());
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
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndBnodeActionEncode(pObj);
  if (pRedoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendRedolog(pTrans, pRedoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING));
  TAOS_RETURN(code);
}

static int32_t mndSetCreateBnodeUndoLogs(STrans *pTrans, SBnodeObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pUndoRaw = mndBnodeActionEncode(pObj);
  if (pUndoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendUndolog(pTrans, pUndoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED));
  TAOS_RETURN(code);
}

static int32_t mndSetCreateBnodeCommitLogs(STrans *pTrans, SBnodeObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndBnodeActionEncode(pObj);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pCommitRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));
  TAOS_RETURN(code);
}

static int32_t mndSetCreateBnodeRedoActions(STrans *pTrans, SDnodeObj *pDnode, SBnodeObj *pObj) {
  int32_t          code = 0;
  SDCreateBnodeReq createReq = {0};
  createReq.dnodeId = pDnode->id;
  createReq.bnodeProto = pObj->proto;

  int32_t contLen = tSerializeSMCreateBnodeReq(NULL, 0, &createReq);
  void   *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    code = terrno;
    TAOS_RETURN(code);
  }
  code = tSerializeSMCreateBnodeReq(pReq, contLen, &createReq);
  if (code < 0) {
    mError("bnode:%d, failed to serialize create drop bnode request since %s", createReq.dnodeId, terrstr());

    taosMemoryFree(pReq);
    TAOS_RETURN(code);
  }

  STransAction action = {0};
  action.epSet = mndGetDnodeEpset(pDnode);
  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_CREATE_BNODE;
  action.acceptableCode = TSDB_CODE_BNODE_ALREADY_DEPLOYED;

  if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pReq);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

static int32_t mndSetCreateBnodeUndoActions(STrans *pTrans, SDnodeObj *pDnode, SBnodeObj *pObj) {
  int32_t        code = 0;
  SDDropBnodeReq dropReq = {0};
  dropReq.dnodeId = pDnode->id;

  int32_t contLen = tSerializeSMDropBnodeReq(NULL, 0, &dropReq);
  void   *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    code = terrno;
    TAOS_RETURN(code);
  }
  code = tSerializeSMDropBnodeReq(pReq, contLen, &dropReq);
  if (code < 0) {
    mError("bnode:%d, failed to serialize create drop bnode request since %s", dropReq.dnodeId, terrstr());
  }

  STransAction action = {0};
  action.epSet = mndGetDnodeEpset(pDnode);
  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_DROP_BNODE;
  action.acceptableCode = TSDB_CODE_BNODE_NOT_DEPLOYED;

  if ((code = mndTransAppendUndoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pReq);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

static int32_t mndCreateBnode(SMnode *pMnode, SRpcMsg *pReq, SDnodeObj *pDnode, SMCreateBnodeReq *pCreate) {
  int32_t code = -1;

  SBnodeObj bnodeObj = {0};
  bnodeObj.id = pDnode->id;
  bnodeObj.proto = pCreate->bnodeProto;
  bnodeObj.createdTime = taosGetTimestampMs();
  bnodeObj.updateTime = bnodeObj.createdTime;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "create-bnode");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }
  mndTransSetSerial(pTrans);

  mInfo("trans:%d, to create bnode:%d %d", pTrans->id, pCreate->dnodeId, pCreate->bnodeProto);

  TAOS_CHECK_GOTO(mndSetCreateBnodeRedoLogs(pTrans, &bnodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateBnodeUndoLogs(pTrans, &bnodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateBnodeCommitLogs(pTrans, &bnodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateBnodeRedoActions(pTrans, pDnode, &bnodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateBnodeUndoActions(pTrans, pDnode, &bnodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndProcessCreateBnodeReq(SRpcMsg *pReq) {
  SMnode          *pMnode = pReq->info.node;
  int32_t          code = -1;
  SBnodeObj       *pObj = NULL;
  SDnodeObj       *pDnode = NULL;
  SMCreateBnodeReq createReq = {0};

  TAOS_CHECK_GOTO(tDeserializeSMCreateBnodeReq(pReq->pCont, pReq->contLen, &createReq), NULL, _OVER);

  mInfo("bnode:%d, start to create", createReq.dnodeId);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CREATE_BNODE), NULL, _OVER);

  pObj = mndAcquireBnode(pMnode, createReq.dnodeId);
  if (pObj != NULL) {
    code = terrno = TSDB_CODE_MND_BNODE_ALREADY_EXIST;
    goto _OVER;
  } else if (terrno != TSDB_CODE_MND_BNODE_NOT_EXIST) {
    code = terrno;
    goto _OVER;
  }

  pDnode = mndAcquireDnode(pMnode, createReq.dnodeId);
  if (pDnode == NULL) {
    code = TSDB_CODE_MND_DNODE_NOT_EXIST;
    goto _OVER;
  }

  code = mndCreateBnode(pMnode, pReq, pDnode, &createReq);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("bnode:%d, failed to create since %s", createReq.dnodeId, tstrerror(code));
    TAOS_RETURN(code);
  }

  //  mndReleaseBnode(pMnode, pObj);
  mndReleaseDnode(pMnode, pDnode);
  tFreeSMCreateBnodeReq(&createReq);
  TAOS_RETURN(code);
}

static int32_t mndSetDropBnodeRedoLogs(STrans *pTrans, SBnodeObj *pObj) {
  int32_t code = 0, lino = 0;

  SSdbRaw *pRedoRaw = mndBnodeActionEncode(pObj);
  if (pRedoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_GOTO(mndTransAppendGroupRedolog(pTrans, pRedoRaw, -1), &lino, _OVER);
  TAOS_CHECK_GOTO(sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING), &lino, _OVER);

_OVER:
  if (code != 0) {
    mError("bnode:%d, failed to drop bnode at line:%d since %s", pObj->id, lino, tstrerror(code));
  }

  TAOS_RETURN(code);
}

static int32_t mndSetDropBnodeCommitLogs(STrans *pTrans, SBnodeObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndBnodeActionEncode(pObj);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pCommitRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED));
  TAOS_RETURN(code);
}

static int32_t mndSetDropBnodeRedoActions(STrans *pTrans, SDnodeObj *pDnode, SBnodeObj *pObj) {
  int32_t        code = 0;
  SDDropBnodeReq dropReq = {0};
  dropReq.dnodeId = pDnode->id;

  int32_t contLen = tSerializeSMDropBnodeReq(NULL, 0, &dropReq);
  void   *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    code = terrno;
    TAOS_RETURN(code);
  }
  code = tSerializeSMDropBnodeReq(pReq, contLen, &dropReq);
  if (code < 0) {
    mError("bnode:%d, failed to serialize create drop bnode request since %s", dropReq.dnodeId, terrstr());
  }

  STransAction action = {0};
  action.epSet = mndGetDnodeEpset(pDnode);
  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_DROP_BNODE;
  action.acceptableCode = TSDB_CODE_BNODE_NOT_DEPLOYED;

  if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pReq);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

int32_t mndSetDropBnodeInfoToTrans(SMnode *pMnode, STrans *pTrans, SBnodeObj *pObj, bool force) {
  int32_t code = -1, lino = 0;

  if (pObj == NULL) return 0;
  TAOS_CHECK_GOTO(mndSetDropBnodeRedoLogs(pTrans, pObj), &lino, _OVER);
  TAOS_CHECK_GOTO(mndSetDropBnodeCommitLogs(pTrans, pObj), &lino, _OVER);
  if (!force) {
    TAOS_CHECK_GOTO(mndSetDropBnodeRedoActions(pTrans, pObj->pDnode, pObj), &lino, _OVER);
  }

_OVER:
  if (code != 0) {
    mError("bnode:%d, failed to drop bnode at line:%d since %s", pObj->id, lino, tstrerror(code));
  }

  return code;
}

static int32_t mndDropBnode(SMnode *pMnode, SRpcMsg *pReq, SBnodeObj *pObj) {
  int32_t code = -1;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq, "drop-bnode");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }
  mndTransSetSerial(pTrans);

  mInfo("trans:%d, used to drop bnode:%d", pTrans->id, pObj->id);
  TAOS_CHECK_GOTO(mndSetDropBnodeInfoToTrans(pMnode, pTrans, pObj, false), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndProcessDropBnodeReq(SRpcMsg *pReq) {
  SMnode        *pMnode = pReq->info.node;
  int32_t        code = -1;
  SBnodeObj     *pObj = NULL;
  SDnodeObj     *pDnode = NULL;
  SMDropBnodeReq dropReq = {0};

  TAOS_CHECK_GOTO(tDeserializeSMDropBnodeReq(pReq->pCont, pReq->contLen, &dropReq), NULL, _OVER);

  mInfo("bnode:%d, start to drop", dropReq.dnodeId);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_DROP_BNODE), NULL, _OVER);

  if (dropReq.dnodeId <= 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  pDnode = mndAcquireDnode(pMnode, dropReq.dnodeId);
  if (pDnode == NULL) {
    code = TSDB_CODE_MND_DNODE_NOT_EXIST;
    goto _OVER;
  }

  if (!mndIsDnodeOnline(pDnode, taosGetTimestampMs())) {
    code = TSDB_CODE_DNODE_OFFLINE;
    goto _OVER;
  }

  pObj = mndAcquireBnode(pMnode, dropReq.dnodeId);
  if (pObj == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }

  // check deletable
  code = mndDropBnode(pMnode, pReq, pObj);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("bnode:%d, failed to drop since %s", dropReq.dnodeId, tstrerror(code));
  }

  mndReleaseBnode(pMnode, pObj);
  mndReleaseDnode(pMnode, pDnode);
  tFreeSMDropBnodeReq(&dropReq);
  TAOS_RETURN(code);
}

static const char *mndBnodeProtoStr(int32_t proto) {
  switch (proto) {
    case TSDB_BNODE_OPT_PROTO_MQTT:
      return TSDB_BNODE_OPT_PROTO_STR_MQTT;
    default:
      break;
  }
  return "unknown";
}

static int32_t mndRetrieveBnodes(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode    *pMnode = pReq->info.node;
  SSdb      *pSdb = pMnode->pSdb;
  int32_t    numOfRows = 0;
  int32_t    cols = 0;
  SBnodeObj *pObj = NULL;
  char       buf[TSDB_EP_LEN + VARSTR_HEADER_SIZE];

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_BNODE, pShow->pIter, (void **)&pObj);
    if (pShow->pIter == NULL) break;

    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_RETURN_WITH_RELEASE(colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->id, false), pSdb, pObj);

    char mqtt_ep[TSDB_EP_LEN] = {0};
    char ep[TSDB_EP_LEN + VARSTR_HEADER_SIZE] = {0};

    TAOS_UNUSED(tsnprintf(mqtt_ep, TSDB_EP_LEN - 1, "%s:%hu", pObj->pDnode->fqdn, tsMqttPort));
    STR_WITH_MAXSIZE_TO_VARSTR(ep, mqtt_ep, pShow->pMeta->pSchemas[cols].bytes);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_RETURN_WITH_RELEASE(colDataSetVal(pColInfo, numOfRows, (const char *)ep, false), pSdb, pObj);

    STR_TO_VARSTR(buf, mndBnodeProtoStr(pObj->proto));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_RETURN_WITH_RELEASE(colDataSetVal(pColInfo, numOfRows, (const char *)buf, false), pSdb, pObj);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TAOS_CHECK_RETURN_WITH_RELEASE(colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->createdTime, false), pSdb,
                                   pObj);

    numOfRows++;
    sdbRelease(pSdb, pObj);
  }

  pShow->numOfRows += numOfRows;

  return numOfRows;
}

static void mndCancelGetNextBnode(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_BNODE);
}

SEpSet mndAcquireEpFromBnode(SMnode *pMnode, const SBnodeObj *pBnode) {
  SEpSet epSet = {.numOfEps = 1, .inUse = 0};
  memcpy(epSet.eps[0].fqdn, pBnode->pDnode->fqdn, TSDB_FQDN_LEN);
  epSet.eps[0].port = pBnode->pDnode->port;
  return epSet;
}

SBnodeObj *mndAcquireBnode(SMnode *pMnode, int32_t dnodeId) {
  SBnodeObj *pObj = sdbAcquire(pMnode->pSdb, SDB_BNODE, &dnodeId);
  if (pObj == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_MND_BNODE_NOT_EXIST;
  }
  return pObj;
}

void mndReleaseBnode(SMnode *pMnode, SBnodeObj *pObj) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pObj);
}

int32_t mndInitBnode(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_BNODE,
      .keyType = SDB_KEY_INT32,
      .encodeFp = (SdbEncodeFp)mndBnodeActionEncode,
      .decodeFp = (SdbDecodeFp)mndBnodeActionDecode,
      .insertFp = (SdbInsertFp)mndBnodeActionInsert,
      .updateFp = (SdbUpdateFp)mndBnodeActionUpdate,
      .deleteFp = (SdbDeleteFp)mndBnodeActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_BNODE, mndProcessCreateBnodeReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_BNODE, mndProcessDropBnodeReq);
  mndSetMsgHandle(pMnode, TDMT_DND_CREATE_BNODE_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_DND_DROP_BNODE_RSP, mndTransProcessRsp);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_BNODE, mndRetrieveBnodes);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_BNODE, mndCancelGetNextBnode);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupBnode(SMnode *pMnode) {}
