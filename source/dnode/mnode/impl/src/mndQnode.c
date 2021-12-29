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
#include "mndDnode.h"
#include "mndShow.h"
#include "mndTrans.h"

#define TSDB_QNODE_VER_NUMBER 1
#define TSDB_QNODE_RESERVE_SIZE 64

static SSdbRaw *mndQnodeActionEncode(SQnodeObj *pObj);
static SSdbRow *mndQnodeActionDecode(SSdbRaw *pRaw);
static int32_t  mndQnodeActionInsert(SSdb *pSdb, SQnodeObj *pObj);
static int32_t  mndQnodeActionDelete(SSdb *pSdb, SQnodeObj *pObj);
static int32_t  mndQnodeActionUpdate(SSdb *pSdb, SQnodeObj *pOldQnode, SQnodeObj *pNewQnode);
static int32_t  mndProcessCreateQnodeReq(SMnodeMsg *pMsg);
static int32_t  mndProcessDropQnodeReq(SMnodeMsg *pMsg);
static int32_t  mndProcessCreateQnodeRsp(SMnodeMsg *pMsg);
static int32_t  mndProcessDropQnodeRsp(SMnodeMsg *pMsg);
static int32_t  mndGetQnodeMeta(SMnodeMsg *pMsg, SShowObj *pShow, STableMetaMsg *pMeta);
static int32_t  mndRetrieveQnodes(SMnodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows);
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

  mndAddShowMetaHandle(pMnode, TSDB_MGMT_TABLE_QNODE, mndGetQnodeMeta);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_QNODE, mndRetrieveQnodes);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_QNODE, mndCancelGetNextQnode);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupQnode(SMnode *pMnode) {}

static SQnodeObj *mndAcquireQnode(SMnode *pMnode, int32_t qnodeId) {
  SSdb      *pSdb = pMnode->pSdb;
  SQnodeObj *pObj = sdbAcquire(pSdb, SDB_QNODE, &qnodeId);
  if (pObj == NULL) {
    terrno = TSDB_CODE_MND_QNODE_NOT_EXIST;
  }
  return pObj;
}

static void mndReleaseQnode(SMnode *pMnode, SQnodeObj *pObj) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pObj);
}

static SSdbRaw *mndQnodeActionEncode(SQnodeObj *pObj) {
  SSdbRaw *pRaw = sdbAllocRaw(SDB_QNODE, TSDB_QNODE_VER_NUMBER, sizeof(SQnodeObj) + TSDB_QNODE_RESERVE_SIZE);
  if (pRaw == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pObj->id);
  SDB_SET_INT64(pRaw, dataPos, pObj->createdTime)
  SDB_SET_INT64(pRaw, dataPos, pObj->updateTime)
  SDB_SET_RESERVE(pRaw, dataPos, TSDB_QNODE_RESERVE_SIZE)

  return pRaw;
}

static SSdbRow *mndQnodeActionDecode(SSdbRaw *pRaw) {
  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) return NULL;

  if (sver != TSDB_QNODE_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("failed to decode qnode since %s", terrstr());
    return NULL;
  }

  SSdbRow   *pRow = sdbAllocRow(sizeof(SQnodeObj));
  SQnodeObj *pObj = sdbGetRowObj(pRow);
  if (pObj == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, pRow, dataPos, &pObj->id)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pObj->createdTime)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pObj->updateTime)
  SDB_GET_RESERVE(pRaw, pRow, dataPos, TSDB_QNODE_RESERVE_SIZE)

  return pRow;
}

static int32_t mndQnodeActionInsert(SSdb *pSdb, SQnodeObj *pObj) {
  mTrace("qnode:%d, perform insert action", pObj->id);
  pObj->pDnode = sdbAcquire(pSdb, SDB_DNODE, &pObj->id);
  if (pObj->pDnode == NULL) {
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    mError("qnode:%d, failed to perform insert action since %s", pObj->id, terrstr());
    return -1;
  }

  return 0;
}

static int32_t mndQnodeActionDelete(SSdb *pSdb, SQnodeObj *pObj) {
  mTrace("qnode:%d, perform delete action", pObj->id);
  if (pObj->pDnode != NULL) {
    sdbRelease(pSdb, pObj->pDnode);
    pObj->pDnode = NULL;
  }

  return 0;
}

static int32_t mndQnodeActionUpdate(SSdb *pSdb, SQnodeObj *pOldQnode, SQnodeObj *pNewQnode) {
  mTrace("qnode:%d, perform update action", pOldQnode->id);
  pOldQnode->updateTime = pNewQnode->updateTime;
  return 0;
}

static int32_t mndSetCreateQnodeRedoLogs(STrans *pTrans, SQnodeObj *pObj) {
  SSdbRaw *pRedoRaw = mndQnodeActionEncode(pObj);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING) != 0) return -1;
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
  SDCreateQnodeMsg *pMsg = malloc(sizeof(SDCreateQnodeMsg));
  if (pMsg == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  pMsg->dnodeId = htonl(pMsg->dnodeId);

  STransAction action = {0};
  action.epSet = mndGetDnodeEpset(pDnode);
  action.pCont = pMsg;
  action.contLen = sizeof(SDCreateQnodeMsg);
  action.msgType = TDMT_DND_CREATE_QNODE;

  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    free(pMsg);
    return -1;
  }

  return 0;
}

static int32_t mndCreateQnode(SMnode *pMnode, SMnodeMsg *pMsg, SDnodeObj *pDnode, SMCreateQnodeMsg *pCreate) {
  SQnodeObj qnodeObj = {0};
  qnodeObj.id = pDnode->id;
  qnodeObj.createdTime = taosGetTimestampMs();
  qnodeObj.updateTime = qnodeObj.createdTime;

  int32_t code = -1;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, &pMsg->rpcMsg);
  if (pTrans == NULL) {
    mError("qnode:%d, failed to create since %s", pCreate->dnodeId, terrstr());
    goto CREATE_QNODE_OVER;
  }
  mDebug("trans:%d, used to create qnode:%d", pTrans->id, pCreate->dnodeId);

  if (mndSetCreateQnodeRedoLogs(pTrans, &qnodeObj) != 0) {
    mError("trans:%d, failed to set redo log since %s", pTrans->id, terrstr());
    goto CREATE_QNODE_OVER;
  }

  if (mndSetCreateQnodeCommitLogs(pTrans, &qnodeObj) != 0) {
    mError("trans:%d, failed to set commit log since %s", pTrans->id, terrstr());
    goto CREATE_QNODE_OVER;
  }

  if (mndSetCreateQnodeRedoActions(pTrans, pDnode, &qnodeObj) != 0) {
    mError("trans:%d, failed to set redo actions since %s", pTrans->id, terrstr());
    goto CREATE_QNODE_OVER;
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    goto CREATE_QNODE_OVER;
  }

  code = 0;

CREATE_QNODE_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessCreateQnodeReq(SMnodeMsg *pMsg) {
  SMnode           *pMnode = pMsg->pMnode;
  SMCreateQnodeMsg *pCreate = pMsg->rpcMsg.pCont;

  pCreate->dnodeId = htonl(pCreate->dnodeId);

  mDebug("qnode:%d, start to create", pCreate->dnodeId);

  SQnodeObj *pObj = mndAcquireQnode(pMnode, pCreate->dnodeId);
  if (pObj != NULL) {
    mError("qnode:%d, qnode already exist", pObj->id);
    mndReleaseQnode(pMnode, pObj);
    return -1;
  }

  SDnodeObj *pDnode = mndAcquireDnode(pMnode, pCreate->dnodeId);
  if (pDnode == NULL) {
    mError("qnode:%d, dnode not exist", pCreate->dnodeId);
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    return -1;
  }

  int32_t code = mndCreateQnode(pMnode, pMsg, pDnode, pCreate);
  mndReleaseDnode(pMnode, pDnode);

  if (code != 0) {
    mError("qnode:%d, failed to create since %s", pCreate->dnodeId, terrstr());
    return -1;
  }

  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
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
  SDDropQnodeMsg *pMsg = malloc(sizeof(SDDropQnodeMsg));
  if (pMsg == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  pMsg->dnodeId = htonl(pMsg->dnodeId);

  STransAction action = {0};
  action.epSet = mndGetDnodeEpset(pDnode);
  action.pCont = pMsg;
  action.contLen = sizeof(SDDropQnodeMsg);
  action.msgType = TDMT_DND_DROP_QNODE;

  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    free(pMsg);
    return -1;
  }

  return 0;
}

static int32_t mndDropQnode(SMnode *pMnode, SMnodeMsg *pMsg, SQnodeObj *pObj) {
  int32_t code = -1;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, &pMsg->rpcMsg);
  if (pTrans == NULL) {
    mError("qnode:%d, failed to drop since %s", pObj->id, terrstr());
    goto DROP_QNODE_OVER;
  }

  mDebug("trans:%d, used to drop qnode:%d", pTrans->id, pObj->id);

  if (mndSetDropQnodeRedoLogs(pTrans, pObj) != 0) {
    mError("trans:%d, failed to set redo log since %s", pTrans->id, terrstr());
    goto DROP_QNODE_OVER;
  }

  if (mndSetDropQnodeCommitLogs(pTrans, pObj) != 0) {
    mError("trans:%d, failed to set commit log since %s", pTrans->id, terrstr());
    goto DROP_QNODE_OVER;
  }

  if (mndSetDropQnodeRedoActions(pTrans, pObj->pDnode, pObj) != 0) {
    mError("trans:%d, failed to set redo actions since %s", pTrans->id, terrstr());
    goto DROP_QNODE_OVER;
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    goto DROP_QNODE_OVER;
  }

  code = 0;

DROP_QNODE_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessDropQnodeReq(SMnodeMsg *pMsg) {
  SMnode         *pMnode = pMsg->pMnode;
  SMDropQnodeMsg *pDrop = pMsg->rpcMsg.pCont;
  pDrop->dnodeId = htonl(pDrop->dnodeId);

  mDebug("qnode:%d, start to drop", pDrop->dnodeId);

  if (pDrop->dnodeId <= 0) {
    terrno = TSDB_CODE_SDB_APP_ERROR;
    mError("qnode:%d, failed to drop since %s", pDrop->dnodeId, terrstr());
    return -1;
  }

  SQnodeObj *pObj = mndAcquireQnode(pMnode, pDrop->dnodeId);
  if (pObj == NULL) {
    mError("qnode:%d, not exist", pDrop->dnodeId);
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    return -1;
  }

  int32_t code = mndDropQnode(pMnode, pMsg, pObj);
  if (code != 0) {
    mError("qnode:%d, failed to drop since %s", pMnode->dnodeId, terrstr());
    return -1;
  }

  sdbRelease(pMnode->pSdb, pMnode);
  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static int32_t mndProcessCreateQnodeRsp(SMnodeMsg *pMsg) {
  mndTransProcessRsp(pMsg);
  return 0;
}

static int32_t mndProcessDropQnodeRsp(SMnodeMsg *pMsg) {
  mndTransProcessRsp(pMsg);
  return 0;
}

static int32_t mndGetQnodeMeta(SMnodeMsg *pMsg, SShowObj *pShow, STableMetaMsg *pMeta) {
  SMnode *pMnode = pMsg->pMnode;
  SSdb   *pSdb = pMnode->pSdb;

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->pSchema;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "id");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = TSDB_EP_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "endpoint");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create_time");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htonl(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = sdbGetSize(pSdb, SDB_QNODE);
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  strcpy(pMeta->tbFname, mndShowStr(pShow->type));

  return 0;
}

static int32_t mndRetrieveQnodes(SMnodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows) {
  SMnode    *pMnode = pMsg->pMnode;
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
