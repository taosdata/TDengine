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
#include "mndXnode.h"
#include "mndDnode.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndTrans.h"
#include "mndUser.h"

#define XNODE_VER_NUMBER   1
#define XNODE_RESERVE_SIZE 64

static SSdbRaw *mndXnodeActionEncode(SXnodeObj *pObj) {
  int32_t code = 0;
  int32_t lino = 0;

  terrno = TSDB_CODE_OUT_OF_MEMORY;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_XNODE, XNODE_VER_NUMBER, sizeof(SXnodeObj) + XNODE_RESERVE_SIZE);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pObj->id, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->proto, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->createdTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->updateTime, _OVER)
  SDB_SET_RESERVE(pRaw, dataPos, XNODE_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("xnode:%d, failed to encode to raw:%p since %s", pObj->id, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("xnode:%d, encode to raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRaw;
}

static SSdbRow *mndXnodeActionDecode(SSdbRaw *pRaw) {
  int32_t    code = 0;
  int32_t    lino = 0;
  SSdbRow   *pRow = NULL;
  SXnodeObj *pObj = NULL;
  int8_t     sver = 0;

  terrno = TSDB_CODE_OUT_OF_MEMORY;

  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver != XNODE_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  pRow = sdbAllocRow(sizeof(SXnodeObj));
  if (pRow == NULL) goto _OVER;

  pObj = sdbGetRowObj(pRow);
  if (pObj == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &pObj->id, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pObj->proto, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->createdTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->updateTime, _OVER)
  SDB_GET_RESERVE(pRaw, dataPos, XNODE_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("xnode:%d, failed to decode from raw:%p since %s", pObj == NULL ? 0 : pObj->id, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("xnode:%d, decode from raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRow;
}

static int32_t mndXnodeActionInsert(SSdb *pSdb, SXnodeObj *pObj) {
  mTrace("xnode:%d, perform insert action, row:%p", pObj->id, pObj);
  pObj->pDnode = sdbAcquire(pSdb, SDB_DNODE, &pObj->id);
  if (pObj->pDnode == NULL) {
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    mError("xnode:%d, failed to perform insert action since %s", pObj->id, terrstr());
    return -1;
  }

  return 0;
}

static int32_t mndXnodeActionDelete(SSdb *pSdb, SXnodeObj *pObj) {
  mTrace("xnode:%d, perform delete action, row:%p", pObj->id, pObj);
  if (pObj->pDnode != NULL) {
    sdbRelease(pSdb, pObj->pDnode);
    pObj->pDnode = NULL;
  }

  return 0;
}

static int32_t mndXnodeActionUpdate(SSdb *pSdb, SXnodeObj *pOld, SXnodeObj *pNew) {
  mTrace("xnode:%d, perform update action, old row:%p new row:%p", pOld->id, pOld, pNew);
  pOld->updateTime = pNew->updateTime;
  return 0;
}

static int32_t mndSetCreateXnodeRedoLogs(STrans *pTrans, SXnodeObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndXnodeActionEncode(pObj);
  if (pRedoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendRedolog(pTrans, pRedoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING));
  TAOS_RETURN(code);
}

static int32_t mndSetCreateXnodeUndoLogs(STrans *pTrans, SXnodeObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pUndoRaw = mndXnodeActionEncode(pObj);
  if (pUndoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendUndolog(pTrans, pUndoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED));
  TAOS_RETURN(code);
}

static int32_t mndSetCreateXnodeCommitLogs(STrans *pTrans, SXnodeObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndXnodeActionEncode(pObj);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pCommitRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));
  TAOS_RETURN(code);
}

static int32_t mndSetCreateXnodeRedoActions(STrans *pTrans, SDnodeObj *pDnode, SXnodeObj *pObj) {
  int32_t          code = 0;
  SDCreateXnodeReq createReq = {0};
  createReq.dnodeId = pDnode->id;
  createReq.xnodeProto = pObj->proto;

  int32_t contLen = tSerializeSMCreateXnodeReq(NULL, 0, &createReq);
  void   *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    code = terrno;
    TAOS_RETURN(code);
  }
  code = tSerializeSMCreateXnodeReq(pReq, contLen, &createReq);
  if (code < 0) {
    mError("xnode:%d, failed to serialize create drop xnode request since %s", createReq.dnodeId, terrstr());
  }

  STransAction action = {0};
  action.epSet = mndGetDnodeEpset(pDnode);
  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_CREATE_XNODE;
  action.acceptableCode = TSDB_CODE_XNODE_ALREADY_DEPLOYED;

  if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pReq);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

static int32_t mndSetCreateXnodeUndoActions(STrans *pTrans, SDnodeObj *pDnode, SXnodeObj *pObj) {
  int32_t        code = 0;
  SDDropXnodeReq dropReq = {0};
  dropReq.dnodeId = pDnode->id;

  int32_t contLen = tSerializeSMDropXnodeReq(NULL, 0, &dropReq);
  void   *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    code = terrno;
    TAOS_RETURN(code);
  }
  code = tSerializeSMDropXnodeReq(pReq, contLen, &dropReq);
  if (code < 0) {
    mError("xnode:%d, failed to serialize create drop xnode request since %s", dropReq.dnodeId, terrstr());
  }

  STransAction action = {0};
  action.epSet = mndGetDnodeEpset(pDnode);
  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_DROP_XNODE;
  action.acceptableCode = TSDB_CODE_XNODE_NOT_DEPLOYED;

  if ((code = mndTransAppendUndoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pReq);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

static int32_t mndCreateXnode(SMnode *pMnode, SRpcMsg *pReq, SDnodeObj *pDnode, SMCreateXnodeReq *pCreate) {
  int32_t code = -1;

  SXnodeObj xnodeObj = {0};
  xnodeObj.id = pDnode->id;
  xnodeObj.proto = pCreate->xnodeProto;
  xnodeObj.createdTime = taosGetTimestampMs();
  xnodeObj.updateTime = xnodeObj.createdTime;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "create-xnode");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }
  mndTransSetSerial(pTrans);

  mInfo("trans:%d, to create xnode:%d %d", pTrans->id, pCreate->dnodeId, pCreate->xnodeProto);

  TAOS_CHECK_GOTO(mndSetCreateXnodeRedoLogs(pTrans, &xnodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateXnodeUndoLogs(pTrans, &xnodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateXnodeCommitLogs(pTrans, &xnodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateXnodeRedoActions(pTrans, pDnode, &xnodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateXnodeUndoActions(pTrans, pDnode, &xnodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndProcessCreateXnodeReq(SRpcMsg *pReq) {
  SMnode          *pMnode = pReq->info.node;
  int32_t          code = -1;
  SXnodeObj       *pObj = NULL;
  SDnodeObj       *pDnode = NULL;
  SMCreateXnodeReq createReq = {0};

  TAOS_CHECK_GOTO(tDeserializeSMCreateXnodeReq(pReq->pCont, pReq->contLen, &createReq), NULL, _OVER);

  mInfo("xnode:%d, start to create", createReq.dnodeId);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CREATE_XNODE), NULL, _OVER);

  pObj = mndAcquireXnode(pMnode, createReq.dnodeId);
  if (pObj != NULL) {
    code = terrno = TSDB_CODE_MND_XNODE_ALREADY_EXIST;
    goto _OVER;
  } else if (terrno != TSDB_CODE_MND_XNODE_NOT_EXIST) {
    code = terrno;
    goto _OVER;
  }
  /*
  if (sdbGetSize(pMnode->pSdb, SDB_XNODE) >= 1) {
    code = TSDB_CODE_MND_XNODE_ALREADY_EXIST;
    goto _OVER;
  }
  */
  pDnode = mndAcquireDnode(pMnode, createReq.dnodeId);
  if (pDnode == NULL) {
    code = TSDB_CODE_MND_DNODE_NOT_EXIST;
    goto _OVER;
  }

  code = mndCreateXnode(pMnode, pReq, pDnode, &createReq);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("xnode:%d, failed to create since %s", createReq.dnodeId, tstrerror(code));
    TAOS_RETURN(code);
  }

  //  mndReleaseXnode(pMnode, pObj);
  mndReleaseDnode(pMnode, pDnode);
  tFreeSMCreateXnodeReq(&createReq);
  TAOS_RETURN(code);
}

static int32_t mndSetDropXnodeRedoLogs(STrans *pTrans, SXnodeObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndXnodeActionEncode(pObj);
  if (pRedoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendRedolog(pTrans, pRedoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING));
  TAOS_RETURN(code);
}

static int32_t mndSetDropXnodeCommitLogs(STrans *pTrans, SXnodeObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndXnodeActionEncode(pObj);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pCommitRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED));
  TAOS_RETURN(code);
}

static int32_t mndSetDropXnodeRedoActions(STrans *pTrans, SDnodeObj *pDnode, SXnodeObj *pObj) {
  int32_t        code = 0;
  SDDropXnodeReq dropReq = {0};
  dropReq.dnodeId = pDnode->id;

  int32_t contLen = tSerializeSMDropXnodeReq(NULL, 0, &dropReq);
  void   *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    code = terrno;
    TAOS_RETURN(code);
  }
  code = tSerializeSMDropXnodeReq(pReq, contLen, &dropReq);
  if (code < 0) {
    mError("xnode:%d, failed to serialize create drop xnode request since %s", dropReq.dnodeId, terrstr());
  }

  STransAction action = {0};
  action.epSet = mndGetDnodeEpset(pDnode);
  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_DROP_XNODE;
  action.acceptableCode = TSDB_CODE_XNODE_NOT_DEPLOYED;

  if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pReq);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

int32_t mndSetDropXnodeInfoToTrans(SMnode *pMnode, STrans *pTrans, SXnodeObj *pObj, bool force) {
  if (pObj == NULL) return 0;
  TAOS_CHECK_RETURN(mndSetDropXnodeRedoLogs(pTrans, pObj));
  TAOS_CHECK_RETURN(mndSetDropXnodeCommitLogs(pTrans, pObj));
  if (!force) {
    TAOS_CHECK_RETURN(mndSetDropXnodeRedoActions(pTrans, pObj->pDnode, pObj));
  }
  return 0;
}

static int32_t mndDropXnode(SMnode *pMnode, SRpcMsg *pReq, SXnodeObj *pObj) {
  int32_t code = -1;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq, "drop-xnode");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }
  mndTransSetSerial(pTrans);

  mInfo("trans:%d, used to drop xnode:%d", pTrans->id, pObj->id);
  TAOS_CHECK_GOTO(mndSetDropXnodeInfoToTrans(pMnode, pTrans, pObj, false), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndProcessDropXnodeReq(SRpcMsg *pReq) {
  SMnode        *pMnode = pReq->info.node;
  int32_t        code = -1;
  SXnodeObj     *pObj = NULL;
  SDnodeObj     *pDnode = NULL;
  SMDropXnodeReq dropReq = {0};

  TAOS_CHECK_GOTO(tDeserializeSMDropXnodeReq(pReq->pCont, pReq->contLen, &dropReq), NULL, _OVER);

  mInfo("xnode:%d, start to drop", dropReq.dnodeId);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_DROP_XNODE), NULL, _OVER);

  if (dropReq.dnodeId <= 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  pDnode = mndAcquireDnode(pMnode, dropReq.dnodeId);
  if (pDnode == NULL) {
    code = TSDB_CODE_MND_DNODE_NOT_EXIST;
    goto _OVER;
  }

  pObj = mndAcquireXnode(pMnode, dropReq.dnodeId);
  if (pObj == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }

  // check deletable
  code = mndDropXnode(pMnode, pReq, pObj);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("xnode:%d, failed to drop since %s", dropReq.dnodeId, tstrerror(code));
  }

  mndReleaseXnode(pMnode, pObj);
  tFreeSMDropXnodeReq(&dropReq);
  TAOS_RETURN(code);
}

static const char *mndXnodeProtoStr(int32_t proto) {
  switch (proto) {
    case TSDB_XNODE_OPT_PROTO_MQTT:
      return TSDB_XNODE_OPT_PROTO_STR_MQTT;
    default:
      break;
  }
  return "unknown";
}

static int32_t mndRetrieveXnodes(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode    *pMnode = pReq->info.node;
  SSdb      *pSdb = pMnode->pSdb;
  int32_t    numOfRows = 0;
  int32_t    cols = 0;
  SXnodeObj *pObj = NULL;
  char       buf[TSDB_EP_LEN + VARSTR_HEADER_SIZE];

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_XNODE, pShow->pIter, (void **)&pObj);
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

    STR_TO_VARSTR(buf, mndXnodeProtoStr(pObj->proto));
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

static void mndCancelGetNextXnode(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_XNODE);
}

SEpSet mndAcquireEpFromXnode(SMnode *pMnode, const SXnodeObj *pXnode) {
  SEpSet epSet = {.numOfEps = 1, .inUse = 0};
  memcpy(epSet.eps[0].fqdn, pXnode->pDnode->fqdn, TSDB_FQDN_LEN);
  epSet.eps[0].port = pXnode->pDnode->port;
  return epSet;
}

SXnodeObj *mndAcquireXnode(SMnode *pMnode, int32_t dnodeId) {
  SXnodeObj *pObj = sdbAcquire(pMnode->pSdb, SDB_XNODE, &dnodeId);
  if (pObj == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_MND_XNODE_NOT_EXIST;
  }
  return pObj;
}

void mndReleaseXnode(SMnode *pMnode, SXnodeObj *pObj) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pObj);
}

int32_t mndInitXnode(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_XNODE,
      .keyType = SDB_KEY_INT32,
      .encodeFp = (SdbEncodeFp)mndXnodeActionEncode,
      .decodeFp = (SdbDecodeFp)mndXnodeActionDecode,
      .insertFp = (SdbInsertFp)mndXnodeActionInsert,
      .updateFp = (SdbUpdateFp)mndXnodeActionUpdate,
      .deleteFp = (SdbDeleteFp)mndXnodeActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_XNODE, mndProcessCreateXnodeReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_XNODE, mndProcessDropXnodeReq);
  mndSetMsgHandle(pMnode, TDMT_DND_CREATE_XNODE_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_DND_DROP_XNODE_RSP, mndTransProcessRsp);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_XNODE, mndRetrieveXnodes);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_XNODE, mndCancelGetNextXnode);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupXnode(SMnode *pMnode) {}
