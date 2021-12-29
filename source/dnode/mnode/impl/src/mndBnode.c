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
#include "mndShow.h"
#include "mndTrans.h"

#define TSDB_BNODE_VER_NUMBER 1
#define TSDB_BNODE_RESERVE_SIZE 64

static SSdbRaw *mndBnodeActionEncode(SBnodeObj *pObj);
static SSdbRow *mndBnodeActionDecode(SSdbRaw *pRaw);
static int32_t  mndBnodeActionInsert(SSdb *pSdb, SBnodeObj *pObj);
static int32_t  mndBnodeActionDelete(SSdb *pSdb, SBnodeObj *pObj);
static int32_t  mndBnodeActionUpdate(SSdb *pSdb, SBnodeObj *pOldBnode, SBnodeObj *pNewBnode);
static int32_t  mndProcessCreateBnodeReq(SMnodeMsg *pMsg);
static int32_t  mndProcessDropBnodeReq(SMnodeMsg *pMsg);
static int32_t  mndProcessCreateBnodeRsp(SMnodeMsg *pMsg);
static int32_t  mndProcessDropBnodeRsp(SMnodeMsg *pMsg);
static int32_t  mndGetBnodeMeta(SMnodeMsg *pMsg, SShowObj *pShow, STableMetaMsg *pMeta);
static int32_t  mndRetrieveBnodes(SMnodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows);
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

  mndAddShowMetaHandle(pMnode, TSDB_MGMT_TABLE_BNODE, mndGetBnodeMeta);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_BNODE, mndRetrieveBnodes);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_BNODE, mndCancelGetNextBnode);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupBnode(SMnode *pMnode) {}

static SBnodeObj *mndAcquireBnode(SMnode *pMnode, int32_t snodeId) {
  SSdb      *pSdb = pMnode->pSdb;
  SBnodeObj *pObj = sdbAcquire(pSdb, SDB_BNODE, &snodeId);
  if (pObj == NULL) {
    terrno = TSDB_CODE_MND_BNODE_NOT_EXIST;
  }
  return pObj;
}

static void mndReleaseBnode(SMnode *pMnode, SBnodeObj *pObj) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pObj);
}

static SSdbRaw *mndBnodeActionEncode(SBnodeObj *pObj) {
  SSdbRaw *pRaw = sdbAllocRaw(SDB_BNODE, TSDB_BNODE_VER_NUMBER, sizeof(SBnodeObj) + TSDB_BNODE_RESERVE_SIZE);
  if (pRaw == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pObj->id);
  SDB_SET_INT64(pRaw, dataPos, pObj->createdTime)
  SDB_SET_INT64(pRaw, dataPos, pObj->updateTime)
  SDB_SET_RESERVE(pRaw, dataPos, TSDB_BNODE_RESERVE_SIZE)

  return pRaw;
}

static SSdbRow *mndBnodeActionDecode(SSdbRaw *pRaw) {
  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) return NULL;

  if (sver != TSDB_BNODE_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("failed to decode snode since %s", terrstr());
    return NULL;
  }

  SSdbRow   *pRow = sdbAllocRow(sizeof(SBnodeObj));
  SBnodeObj *pObj = sdbGetRowObj(pRow);
  if (pObj == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, pRow, dataPos, &pObj->id)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pObj->createdTime)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pObj->updateTime)
  SDB_GET_RESERVE(pRaw, pRow, dataPos, TSDB_BNODE_RESERVE_SIZE)

  return pRow;
}

static int32_t mndBnodeActionInsert(SSdb *pSdb, SBnodeObj *pObj) {
  mTrace("snode:%d, perform insert action", pObj->id);
  pObj->pDnode = sdbAcquire(pSdb, SDB_DNODE, &pObj->id);
  if (pObj->pDnode == NULL) {
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    mError("snode:%d, failed to perform insert action since %s", pObj->id, terrstr());
    return -1;
  }

  return 0;
}

static int32_t mndBnodeActionDelete(SSdb *pSdb, SBnodeObj *pObj) {
  mTrace("snode:%d, perform delete action", pObj->id);
  if (pObj->pDnode != NULL) {
    sdbRelease(pSdb, pObj->pDnode);
    pObj->pDnode = NULL;
  }

  return 0;
}

static int32_t mndBnodeActionUpdate(SSdb *pSdb, SBnodeObj *pOldBnode, SBnodeObj *pNewBnode) {
  mTrace("snode:%d, perform update action", pOldBnode->id);
  pOldBnode->updateTime = pNewBnode->updateTime;
  return 0;
}

static int32_t mndSetCreateBnodeRedoLogs(STrans *pTrans, SBnodeObj *pObj) {
  SSdbRaw *pRedoRaw = mndBnodeActionEncode(pObj);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING) != 0) return -1;
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
  SDCreateBnodeMsg *pMsg = malloc(sizeof(SDCreateBnodeMsg));
  if (pMsg == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  pMsg->dnodeId = htonl(pMsg->dnodeId);

  STransAction action = {0};
  action.epSet = mndGetDnodeEpset(pDnode);
  action.pCont = pMsg;
  action.contLen = sizeof(SDCreateBnodeMsg);
  action.msgType = TDMT_DND_CREATE_BNODE;

  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    free(pMsg);
    return -1;
  }

  return 0;
}

static int32_t mndCreateBnode(SMnode *pMnode, SMnodeMsg *pMsg, SDnodeObj *pDnode, SMCreateBnodeMsg *pCreate) {
  SBnodeObj snodeObj = {0};
  snodeObj.id = pDnode->id;
  snodeObj.createdTime = taosGetTimestampMs();
  snodeObj.updateTime = snodeObj.createdTime;

  int32_t code = -1;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, &pMsg->rpcMsg);
  if (pTrans == NULL) {
    mError("snode:%d, failed to create since %s", pCreate->dnodeId, terrstr());
    goto CREATE_BNODE_OVER;
  }
  mDebug("trans:%d, used to create snode:%d", pTrans->id, pCreate->dnodeId);

  if (mndSetCreateBnodeRedoLogs(pTrans, &snodeObj) != 0) {
    mError("trans:%d, failed to set redo log since %s", pTrans->id, terrstr());
    goto CREATE_BNODE_OVER;
  }

  if (mndSetCreateBnodeCommitLogs(pTrans, &snodeObj) != 0) {
    mError("trans:%d, failed to set commit log since %s", pTrans->id, terrstr());
    goto CREATE_BNODE_OVER;
  }

  if (mndSetCreateBnodeRedoActions(pTrans, pDnode, &snodeObj) != 0) {
    mError("trans:%d, failed to set redo actions since %s", pTrans->id, terrstr());
    goto CREATE_BNODE_OVER;
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    goto CREATE_BNODE_OVER;
  }

  code = 0;

CREATE_BNODE_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessCreateBnodeReq(SMnodeMsg *pMsg) {
  SMnode           *pMnode = pMsg->pMnode;
  SMCreateBnodeMsg *pCreate = pMsg->rpcMsg.pCont;

  pCreate->dnodeId = htonl(pCreate->dnodeId);

  mDebug("snode:%d, start to create", pCreate->dnodeId);

  SBnodeObj *pObj = mndAcquireBnode(pMnode, pCreate->dnodeId);
  if (pObj != NULL) {
    mError("snode:%d, snode already exist", pObj->id);
    mndReleaseBnode(pMnode, pObj);
    return -1;
  }

  SDnodeObj *pDnode = mndAcquireDnode(pMnode, pCreate->dnodeId);
  if (pDnode == NULL) {
    mError("snode:%d, dnode not exist", pCreate->dnodeId);
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    return -1;
  }

  int32_t code = mndCreateBnode(pMnode, pMsg, pDnode, pCreate);
  mndReleaseDnode(pMnode, pDnode);

  if (code != 0) {
    mError("snode:%d, failed to create since %s", pCreate->dnodeId, terrstr());
    return -1;
  }

  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
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
  SDDropBnodeMsg *pMsg = malloc(sizeof(SDDropBnodeMsg));
  if (pMsg == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  pMsg->dnodeId = htonl(pMsg->dnodeId);

  STransAction action = {0};
  action.epSet = mndGetDnodeEpset(pDnode);
  action.pCont = pMsg;
  action.contLen = sizeof(SDDropBnodeMsg);
  action.msgType = TDMT_DND_DROP_BNODE;

  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    free(pMsg);
    return -1;
  }

  return 0;
}

static int32_t mndDropBnode(SMnode *pMnode, SMnodeMsg *pMsg, SBnodeObj *pObj) {
  int32_t code = -1;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, &pMsg->rpcMsg);
  if (pTrans == NULL) {
    mError("snode:%d, failed to drop since %s", pObj->id, terrstr());
    goto DROP_BNODE_OVER;
  }

  mDebug("trans:%d, used to drop snode:%d", pTrans->id, pObj->id);

  if (mndSetDropBnodeRedoLogs(pTrans, pObj) != 0) {
    mError("trans:%d, failed to set redo log since %s", pTrans->id, terrstr());
    goto DROP_BNODE_OVER;
  }

  if (mndSetDropBnodeCommitLogs(pTrans, pObj) != 0) {
    mError("trans:%d, failed to set commit log since %s", pTrans->id, terrstr());
    goto DROP_BNODE_OVER;
  }

  if (mndSetDropBnodeRedoActions(pTrans, pObj->pDnode, pObj) != 0) {
    mError("trans:%d, failed to set redo actions since %s", pTrans->id, terrstr());
    goto DROP_BNODE_OVER;
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    goto DROP_BNODE_OVER;
  }

  code = 0;

DROP_BNODE_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessDropBnodeReq(SMnodeMsg *pMsg) {
  SMnode           *pMnode = pMsg->pMnode;
  SMCreateBnodeMsg *pDrop = pMsg->rpcMsg.pCont;
  pDrop->dnodeId = htonl(pDrop->dnodeId);

  mDebug("snode:%d, start to drop", pDrop->dnodeId);

  if (pDrop->dnodeId <= 0) {
    terrno = TSDB_CODE_SDB_APP_ERROR;
    mError("snode:%d, failed to drop since %s", pDrop->dnodeId, terrstr());
    return -1;
  }

  SBnodeObj *pObj = mndAcquireBnode(pMnode, pDrop->dnodeId);
  if (pObj == NULL) {
    mError("snode:%d, not exist", pDrop->dnodeId);
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    return -1;
  }

  int32_t code = mndDropBnode(pMnode, pMsg, pObj);
  if (code != 0) {
    mError("snode:%d, failed to drop since %s", pMnode->dnodeId, terrstr());
    return -1;
  }

  sdbRelease(pMnode->pSdb, pMnode);
  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static int32_t mndProcessCreateBnodeRsp(SMnodeMsg *pMsg) {
  mndTransProcessRsp(pMsg);
  return 0;
}

static int32_t mndProcessDropBnodeRsp(SMnodeMsg *pMsg) {
  mndTransProcessRsp(pMsg);
  return 0;
}

static int32_t mndGetBnodeMeta(SMnodeMsg *pMsg, SShowObj *pShow, STableMetaMsg *pMeta) {
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

  pShow->numOfRows = sdbGetSize(pSdb, SDB_BNODE);
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  strcpy(pMeta->tbFname, mndShowStr(pShow->type));

  return 0;
}

static int32_t mndRetrieveBnodes(SMnodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows) {
  SMnode    *pMnode = pMsg->pMnode;
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
